import pandas as pd
from pathlib import Path
import os
import json
import argparse
import time
import logging
import sys
from datetime import datetime
from pykis import PyKis
from utils.secret_loader import resolve_secret

# 투자 설정
MAX_RETRIES = 3  # 최대 재시도 횟수
RETRY_DELAY = 1  # 재시도 간 대기 시간 (초)
ORDER_DELAY = 0.5  # 주문 간 대기 시간 (초)
REBALANCE_WAIT_TIME = 60  # 리밸런싱 매도 후 매수 대기 시간 (초)
EXECUTION_LOG_FILE = "portfolio_execution_log.json"  # 실행 기록 파일
REBALANCING_MONTHS = [3, 6, 9, 12]  # 리밸런싱 실행 월
LOG_DIR = "logs"  # 로그 디렉토리

# 전역 로거
logger = None


def setup_logger():
    """
    전역 로거 설정: 콘솔 + 파일 출력
    """
    global logger

    # 로그 디렉토리 생성
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # 로그 파일명: portfolio_YYYYMMDD_HHMMSS.log
    log_filename = os.path.join(
        LOG_DIR,
        f"portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    # 로거 생성
    logger = logging.getLogger('Portfolio')
    logger.setLevel(logging.DEBUG)

    # 기존 핸들러 제거 (중복 방지)
    logger.handlers.clear()

    # 포맷 설정
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"로그 파일: {log_filename}")


def get_latest_portfolio_file():
    """
    portfolio 폴더에서 가장 최신 포트폴리오 파일 경로 반환

    Returns:
        Path: 최신 포트폴리오 파일 경로
    """
    portfolio_dir = Path(__file__).parent / "portfolio"

    if not portfolio_dir.exists():
        raise FileNotFoundError(f"portfolio 폴더가 존재하지 않습니다: {portfolio_dir}")

    # portfolio_*.csv 파일들 찾기
    portfolio_files = list(portfolio_dir.glob("portfolio_*.csv"))

    if not portfolio_files:
        raise FileNotFoundError(f"portfolio 폴더에 포트폴리오 파일이 없습니다: {portfolio_dir}")

    # 파일명에서 날짜 추출하여 가장 최신 파일 찾기
    latest_file = max(portfolio_files, key=lambda f: f.stem.split('_')[1])

    logger.info(f"최신 포트폴리오 파일: {latest_file.name}")
    return latest_file


def calculate_quantities(portfolio_file, total_investment):
    """
    포트폴리오 파일을 읽고 균등 투자 기준으로 매수 수량 계산

    Args:
        portfolio_file: 포트폴리오 CSV 파일 경로
        total_investment: 총 투자액

    Returns:
        pd.DataFrame: 매수 수량이 추가된 포트폴리오
    """
    # 포트폴리오 읽기 (종목코드는 문자열로)
    df = pd.read_csv(portfolio_file, encoding='utf-8-sig', dtype={'code': str})

    # 종목코드 6자리 0 패딩
    df['code'] = df['code'].str.zfill(6)

    num_stocks = len(df)
    amount_per_stock = total_investment / num_stocks

    logger.info(f"\n총 투자액: {total_investment:,}원")
    logger.info(f"종목 수: {num_stocks}개")
    logger.info(f"종목당 투자액: {amount_per_stock:,.0f}원")

    # 수량 계산
    df['투자액'] = amount_per_stock
    df['매수수량'] = (df['투자액'] / df['end_price']).astype(int)
    df['실투자액'] = df['매수수량'] * df['end_price']

    # 컬럼 순서 재정렬
    cols = ['code', '종목명', 'end_price', '매수수량', '투자액', '실투자액',
            'adjusted_momentum_12m', 'fip', 'end_price_date']
    df = df[cols]

    return df


def round_to_tick_size(price):
    """
    주식 호가 단위로 올림

    한국 주식시장 호가 단위:
    - 1,000원 미만: 1원
    - 1,000원 이상 ~ 5,000원 미만: 5원
    - 5,000원 이상 ~ 10,000원 미만: 10원
    - 10,000원 이상 ~ 50,000원 미만: 50원
    - 50,000원 이상 ~ 100,000원 미만: 100원
    - 100,000원 이상 ~ 500,000원 미만: 500원
    - 500,000원 이상: 1,000원

    Args:
        price: 원본 가격

    Returns:
        int: 호가 단위로 올림된 가격
    """
    if price < 1000:
        return price  # 1원 단위
    elif price < 5000:
        return ((price + 4) // 5) * 5  # 5원 단위
    elif price < 10000:
        return ((price + 9) // 10) * 10  # 10원 단위
    elif price < 50000:
        return ((price + 49) // 50) * 50  # 50원 단위
    elif price < 100000:
        return ((price + 99) // 100) * 100  # 100원 단위
    elif price < 500000:
        return ((price + 499) // 500) * 500  # 500원 단위
    else:
        return ((price + 999) // 1000) * 1000  # 1,000원 단위


def load_execution_log():
    """
    실행 기록 파일 로드

    Returns:
        dict: 실행 기록 데이터
    """
    if os.path.exists(EXECUTION_LOG_FILE):
        try:
            with open(EXECUTION_LOG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f" 실행 기록 파일 로드 실패: {e}")
            return {"executions": []}
    else:
        return {"executions": []}


def save_execution_log(log_data):
    """
    실행 기록 파일 저장

    Args:
        log_data: 저장할 실행 기록 데이터
    """
    try:
        with open(EXECUTION_LOG_FILE, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        logger.info(f"실행 기록 저장 완료: {EXECUTION_LOG_FILE}")
    except Exception as e:
        logger.warning(f" 실행 기록 저장 실패: {e}")


def is_rebalancing_month():
    """
    현재 월이 리밸런싱 월(3, 6, 9, 12월)인지 확인

    Returns:
        bool: True면 리밸런싱 월, False면 아님
    """
    current_month = datetime.now().month
    return current_month in REBALANCING_MONTHS


def check_monthly_execution():
    """
    이번 달에 이미 실행되었는지 확인

    Returns:
        bool: True면 이미 실행됨, False면 실행 안됨
    """
    log_data = load_execution_log()
    current_month = datetime.now().strftime("%Y-%m")

    for execution in log_data.get("executions", []):
        if execution.get("month") == current_month and execution.get("success"):
            logger.info(f"\n⚠️  이번 달({current_month})에 이미 실행되었습니다.")
            logger.info(f"   실행일: {execution.get('date')}")
            logger.info(f"   포트폴리오: {execution.get('portfolio_file')}")
            return True

    logger.info(f"\n✅ 이번 달({current_month}) 첫 실행입니다.")
    return False


def record_execution(portfolio_file, success):
    """
    실행 기록 추가

    Args:
        portfolio_file: 포트폴리오 파일명
        success: 실행 성공 여부
    """
    log_data = load_execution_log()

    execution_record = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "month": datetime.now().strftime("%Y-%m"),
        "portfolio_file": str(portfolio_file),
        "success": success,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    log_data["executions"].append(execution_record)
    save_execution_log(log_data)


def initialize_kis(secret_file='secret.json', virtual_file=None):
    """
    PyKis 객체 초기화

    Args:
        secret_file: 실전 계좌 secret 파일 경로
        virtual_file: 모의투자 계좌 secret 파일 경로 (옵션)

    Returns:
        PyKis: 초기화된 PyKis 객체
    """
    if virtual_file:
        logger.info(f"모의투자 모드로 초기화: {secret_file}, {virtual_file}")
        return PyKis(secret_file, virtual_file, keep_token=True)
    else:
        logger.info(f"실전투자 모드로 초기화: {secret_file}")
        return PyKis(secret_file, keep_token=True)


def get_current_holdings(kis):
    """
    현재 보유 종목 및 수량 조회

    Args:
        kis: PyKis 객체

    Returns:
        dict: {종목코드: {'qty': 보유수량, 'name': 종목명}} 딕셔너리
    """
    try:
        holdings = {}
        account = kis.account()
        balance = account.balance()

        # balance 객체에서 보유 종목 정보 추출
        if hasattr(balance, 'stocks') and balance.stocks:
            for stock in balance.stocks:
                # symbol 또는 code 속성 사용
                code = getattr(stock, 'symbol', getattr(stock, 'code', None))
                if code:
                    # 종목코드 6자리 0 패딩
                    code = str(code).zfill(6)
                    qty = int(stock.qty)
                    name = getattr(stock, 'name', '(이름없음)')
                    if qty > 0:
                        holdings[code] = {'qty': qty, 'name': name}

        logger.info(f"\n현재 보유 종목 수: {len(holdings)}개")
        if holdings:
            logger.info("보유 종목 목록:")
            for code, info in holdings.items():
                logger.info(f"  {code} ({info['name']}): {info['qty']}주")

        return holdings

    except Exception as e:
        logger.warning(f" 보유 잔고 조회 실패: {e}")
        logger.info("보유 종목이 없다고 가정하고 진행합니다.")
        return {}


def execute_buy_orders(kis, df_buy, is_virtual=False):
    """
    계산된 수량으로 매수 주문 실행 (리밸런싱 포함, 최우선 지정가, 재시도 로직 포함)

    리밸런싱 로직:
    1. 보유량 > 목표량: (보유량 - 목표량)만큼 시장가 매도
    2. 보유량 < 목표량: (목표량 - 보유량)만큼 최우선 지정가 매수
    3. 보유량 = 목표량: 아무 작업도 하지 않음

    Args:
        kis: PyKis 객체
        df_buy: 매수 계획이 담긴 DataFrame
        is_virtual: 모의투자 여부 (기본: False)

    Returns:
        list: 주문 결과 리스트
    """
    results = []

    # 현재 보유 종목 조회
    holdings = get_current_holdings(kis)

    # 매수 예정 종목 코드 set
    target_codes = set(df_buy['code'].tolist())

    logger.info("\n" + "=" * 80)
    logger.info("매수 주문 실행 (리밸런싱 포함)")
    logger.info("=" * 80)

    # 1단계: 매수 예정에 없는 보유 종목 전량 매도
    non_target_holdings = {code: info for code, info in holdings.items() if code not in target_codes}

    if non_target_holdings:
        logger.info(f"\n[전량 매도] 매수 예정에 없는 보유 종목 {len(non_target_holdings)}개를 매도합니다.")

        for code, info in non_target_holdings.items():
            qty = info['qty']
            logger.info(f"\n[전량 매도] {code}: {qty}주 매도")

            # 시장가 매도 재시도 로직
            sell_success = False
            sell_error = None

            for sell_attempt in range(1, MAX_RETRIES + 1):
                try:
                    if sell_attempt > 1:
                        logger.info(f"[매도 재시도 {sell_attempt}/{MAX_RETRIES}] {code}")
                        time.sleep(RETRY_DELAY * (sell_attempt - 1))

                    # 시장가 전량 매도
                    sell_order = kis.stock(code).sell(price=None, qty=qty, condition=None, execution=None)

                    logger.info(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                    sell_success = True
                    break

                except Exception as e:
                    sell_error = str(e)
                    error_msg = sell_error.lower()

                    # 재시도 불가능한 오류 체크
                    no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                    if any(keyword in error_msg for keyword in no_retry_keywords):
                        logger.info(f"[매도 실패] {code}: {sell_error} (재시도 불가)")
                        break

                    if sell_attempt < MAX_RETRIES:
                        logger.info(f"[매도 오류] {code}: {sell_error} (재시도 예정)")
                    else:
                        logger.info(f"[매도 실패] {code}: {sell_error} (최대 재시도 횟수 초과)")

            if not sell_success:
                results.append({
                    'code': code,
                    'name': '(매수예정외)',
                    'status': 'liquidate_failed',
                    'error': sell_error,
                    'message': '전량 매도 실패',
                    'current_qty': qty,
                    'target_qty': 0
                })
            else:
                results.append({
                    'code': code,
                    'name': '(매수예정외)',
                    'status': 'liquidated',
                    'order': sell_order,
                    'message': f'{qty}주 전량 매도',
                    'current_qty': qty,
                    'target_qty': 0
                })

            # 주문 간 딜레이
            time.sleep(ORDER_DELAY)

        # 전량 매도 후 대기
        if any(r['status'] == 'liquidated' for r in results):
            logger.info(f"\n[대기] 전량 매도 완료 후 {REBALANCE_WAIT_TIME}초 대기...")
            time.sleep(REBALANCE_WAIT_TIME)

    # 2단계: 매수 예정 종목 리밸런싱
    for idx, row in df_buy.iterrows():
        code = row['code']
        name = row['종목명']
        price = int(row['end_price'])
        target_qty = int(row['매수수량'])
        current_qty = holdings.get(code, {}).get('qty', 0)

        # 목표 수량이 0일 때 처리
        if target_qty <= 0:
            if current_qty > 0:
                # 보유량이 있으면 전량 매도
                logger.info(f"[리밸런싱 매도] {code} {name}: 현재 {current_qty}주 → 목표 0주 (전량 매도)")

                # 시장가 매도 재시도 로직
                sell_success = False
                sell_error = None

                for sell_attempt in range(1, MAX_RETRIES + 1):
                    try:
                        if sell_attempt > 1:
                            logger.info(f"[매도 재시도 {sell_attempt}/{MAX_RETRIES}] {code} {name}")
                            time.sleep(RETRY_DELAY * (sell_attempt - 1))

                        # 시장가 전량 매도
                        sell_order = kis.stock(code).sell(price=None, qty=current_qty, condition=None, execution=None)

                        logger.info(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                        sell_success = True
                        break

                    except Exception as e:
                        sell_error = str(e)
                        error_msg = sell_error.lower()

                        # 재시도 불가능한 오류 체크
                        no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                        if any(keyword in error_msg for keyword in no_retry_keywords):
                            logger.info(f"[매도 실패] {code} {name}: {sell_error} (재시도 불가)")
                            break

                        if sell_attempt < MAX_RETRIES:
                            logger.info(f"[매도 오류] {code} {name}: {sell_error} (재시도 예정)")
                        else:
                            logger.info(f"[매도 실패] {code} {name}: {sell_error} (최대 재시도 횟수 초과)")

                if not sell_success:
                    results.append({
                        'code': code,
                        'name': name,
                        'status': 'sell_failed',
                        'error': sell_error,
                        'message': '목표0 전량 매도 실패',
                        'current_qty': current_qty,
                        'target_qty': 0
                    })
                else:
                    results.append({
                        'code': code,
                        'name': name,
                        'status': 'sell_success',
                        'order': sell_order,
                        'message': f'{current_qty}주 전량 매도 (목표0)',
                        'current_qty': current_qty,
                        'target_qty': 0
                    })

                    # 매도 후 대기
                    logger.info(f"[대기] {REBALANCE_WAIT_TIME}초 대기...")
                    time.sleep(REBALANCE_WAIT_TIME)
            else:
                # 보유량도 없으면 스킵
                logger.info(f"[SKIP] {code} {name}: 목표 0, 보유 0 (변동 없음)")
                results.append({
                    'code': code,
                    'name': name,
                    'status': 'skipped',
                    'message': '목표0 보유0'
                })
            continue

        # 수량 차이 계산
        delta = target_qty - current_qty

        if delta == 0:
            # 보유량과 목표량이 같음 - 거래 불필요
            logger.info(f"[유지] {code} {name}: 현재 {current_qty}주 보유, 목표 {target_qty}주 (변동 없음)")
            results.append({
                'code': code,
                'name': name,
                'status': 'unchanged',
                'message': '수량 변동 없음',
                'current_qty': current_qty,
                'target_qty': target_qty
            })
            continue

        elif delta < 0:
            # 보유량 > 목표량 → 매도 필요
            sell_qty = abs(delta)
            logger.info(f"\n[리밸런싱 매도] {code} {name}: 현재 {current_qty}주 → 목표 {target_qty}주 ({sell_qty}주 매도)")

            # 시장가 매도 재시도 로직
            sell_success = False
            sell_error = None

            for sell_attempt in range(1, MAX_RETRIES + 1):
                try:
                    if sell_attempt > 1:
                        logger.info(f"[매도 재시도 {sell_attempt}/{MAX_RETRIES}] {code} {name}")
                        time.sleep(RETRY_DELAY * (sell_attempt - 1))

                    # 시장가 매도
                    sell_order = kis.stock(code).sell(price=None, qty=sell_qty, condition=None, execution=None)

                    logger.info(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                    sell_success = True
                    break

                except Exception as e:
                    sell_error = str(e)
                    error_msg = sell_error.lower()

                    # 재시도 불가능한 오류 체크
                    no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                    if any(keyword in error_msg for keyword in no_retry_keywords):
                        logger.info(f"[매도 실패] {code} {name}: {sell_error} (재시도 불가)")
                        break

                    if sell_attempt < MAX_RETRIES:
                        logger.info(f"[매도 오류] {code} {name}: {sell_error} (재시도 예정)")
                    else:
                        logger.info(f"[매도 실패] {code} {name}: {sell_error} (최대 재시도 횟수 초과)")

            if not sell_success:
                results.append({
                    'code': code,
                    'name': name,
                    'status': 'sell_failed',
                    'error': sell_error,
                    'message': '리밸런싱 매도 실패',
                    'current_qty': current_qty,
                    'target_qty': target_qty
                })
            else:
                results.append({
                    'code': code,
                    'name': name,
                    'status': 'sell_success',
                    'order': sell_order,
                    'message': f'{sell_qty}주 매도',
                    'current_qty': current_qty,
                    'target_qty': target_qty
                })

                # 매도 후 대기
                logger.info(f"[대기] {REBALANCE_WAIT_TIME}초 대기...")
                time.sleep(REBALANCE_WAIT_TIME)

        else:
            # 보유량 < 목표량 → 매수 필요
            buy_qty = delta
            logger.info(f"\n[리밸런싱 매수] {code} {name}: 현재 {current_qty}주 → 목표 {target_qty}주 ({buy_qty}주 매수)")

            # 상한가 계산 (전일 종가의 105%, 호가 단위로 올림)
            max_price = round_to_tick_size(int(price * 1.05))

            # 매수 주문 재시도 로직
            buy_success = False
            last_error = None
            attempt = 0

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    if attempt > 1:
                        logger.info(f"[재시도 {attempt}/{MAX_RETRIES}] {code} {name}")
                        time.sleep(RETRY_DELAY * (attempt - 1))  # 지수 백오프
                    else:
                        if is_virtual:
                            logger.info(f"[매수] {code} {name}: 최유리지정가, 수량={buy_qty}주, 상한가={max_price:,}원 (전일종가: {price:,}원)")
                        else:
                            logger.info(f"[매수] {code} {name}: 최유리지정가, 수량={buy_qty}주 (실전: price=0)")

                    # 최유리지정가 매수 주문
                    # 실전투자: price=0, 모의투자: 상한가 지정
                    order_price = max_price if is_virtual else 0
                    order = kis.stock(code).buy(price=order_price, qty=buy_qty, condition='best', execution=None)

                    logger.info(f"[성공] 주문번호: {order.number if hasattr(order, 'number') else 'N/A'}")
                    results.append({
                        'code': code,
                        'name': name,
                        'status': 'buy_success',
                        'order': order,
                        'attempts': attempt,
                        'message': f'{buy_qty}주 매수',
                        'current_qty': current_qty,
                        'target_qty': target_qty
                    })
                    buy_success = True
                    break

                except Exception as e:
                    last_error = str(e)
                    error_msg = last_error.lower()

                    # 재시도 불가능한 오류 체크
                    no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                    if any(keyword in error_msg for keyword in no_retry_keywords):
                        logger.info(f"[실패] {code} {name}: {last_error} (재시도 불가)")
                        break

                    # 마지막 시도가 아니면 재시도
                    if attempt < MAX_RETRIES:
                        logger.info(f"[오류] {code} {name}: {last_error} (재시도 예정)")
                    else:
                        logger.info(f"[실패] {code} {name}: {last_error} (최대 재시도 횟수 초과)")

            # 실패한 경우 결과 기록
            if not buy_success:
                results.append({
                    'code': code,
                    'name': name,
                    'status': 'buy_failed',
                    'error': last_error,
                    'attempts': attempt,
                    'message': '매수 실패',
                    'current_qty': current_qty,
                    'target_qty': target_qty
                })

        # 주문 간 딜레이 (rate limit 방지)
        if idx < len(df_buy) - 1:  # 마지막 주문이 아닌 경우
            time.sleep(ORDER_DELAY)

    logger.info("=" * 80)

    # 결과 요약
    buy_success_count = sum(1 for r in results if r['status'] == 'buy_success')
    sell_success_count = sum(1 for r in results if r['status'] == 'sell_success')
    liquidated_count = sum(1 for r in results if r['status'] == 'liquidated')
    buy_failed_count = sum(1 for r in results if r['status'] == 'buy_failed')
    sell_failed_count = sum(1 for r in results if r['status'] == 'sell_failed')
    liquidate_failed_count = sum(1 for r in results if r['status'] == 'liquidate_failed')
    unchanged_count = sum(1 for r in results if r['status'] == 'unchanged')
    skipped_count = sum(1 for r in results if r['status'] == 'skipped')

    logger.info(f"\n주문 결과 요약:")
    if liquidated_count > 0:
        logger.info(f"  전량 매도: {liquidated_count}건 (매수예정외 종목)")
    logger.info(f"  매수 성공: {buy_success_count}건")
    logger.info(f"  매도 성공: {sell_success_count}건 (리밸런싱)")
    logger.info(f"  수량 유지: {unchanged_count}건")
    if buy_failed_count > 0:
        logger.info(f"  매수 실패: {buy_failed_count}건")
    if sell_failed_count > 0:
        logger.info(f"  매도 실패: {sell_failed_count}건")
    if liquidate_failed_count > 0:
        logger.info(f"  전량 매도 실패: {liquidate_failed_count}건")
    if skipped_count > 0:
        logger.info(f"  건너뜀: {skipped_count}건")

    # 재시도 통계
    retry_count = sum(1 for r in results if r.get('attempts', 1) > 1)
    if retry_count > 0:
        logger.info(f"\n재시도 성공: {retry_count}건")

    return results


def main():
    # 명령줄 인수 파싱
    parser = argparse.ArgumentParser(description='포트폴리오 매수 계획 생성 및 주문 실행 (3, 6, 9, 12월 첫 거래일 자동 실행)')
    parser.add_argument('--execute', action='store_true', help='실제 매수 주문 실행 (기본: 계획만 출력)')
    parser.add_argument('--secret', required=True, help='실전 계좌 secret 파일 경로 (필수)')
    parser.add_argument('--virtual', default=None, help='모의투자 계좌 secret 파일 경로 (옵션)')
    parser.add_argument('--investment', type=int, default=None, help='총 투자액 (원 단위, 기본: 현재 총평가금액 사용)')
    parser.add_argument('--force', action='store_true', help='이번 달 실행 기록 무시하고 강제 실행')
    args = parser.parse_args()

    # 시크릿 파일 경로 resolve (GCP 모드 지원)
    args.secret = resolve_secret(args.secret)
    if args.virtual:
        args.virtual = resolve_secret(args.virtual)

    # 로거 설정
    setup_logger()

    logger.info("=" * 80)
    logger.info("포트폴리오 리밸런싱")
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    # --force 옵션 처리
    if args.force:
        logger.info("\n⚠️  --force 옵션: 리밸런싱 월 및 실행 기록 확인을 건너뜁니다.")
    else:
        # 1. 리밸런싱 월 확인 (3, 6, 9, 12월)
        if not is_rebalancing_month():
            current_month = datetime.now().month
            logger.info(f"\n⏭️  현재 월({current_month}월)은 리밸런싱 월이 아닙니다.")
            logger.info(f"   리밸런싱 월: {', '.join(map(str, REBALANCING_MONTHS))}월")
            logger.info("\n종료합니다.")
            return

        logger.info(f"\n✅ 현재 월({datetime.now().month}월)은 리밸런싱 월입니다.")

        # 2. 실행 기록 확인 (--execute 모드일 때만)
        if args.execute:
            if check_monthly_execution():
                logger.info("\n이미 실행되었으므로 종료합니다.")
                logger.info("강제 실행하려면 --force 옵션을 사용하세요.")
                return

    # 최신 포트폴리오 파일 찾기
    portfolio_file = get_latest_portfolio_file()

    # 투자액 결정
    kis = None
    if args.investment is None:
        # 모의투자 모드에서는 총평가금액 API가 작동하지 않으므로 투자액 필수
        if args.virtual:
            logger.info("\n[오류] 모의투자 모드에서는 --investment 옵션으로 투자액을 지정해야 합니다.")
            logger.info("예: python buy_portfolio.py --virtual secret_virtual.json --investment 10000000")
            return

        # 실전투자 모드에서만 현재 총평가금액 조회
        logger.info("\n투자액 설정: 현재 총평가금액 사용 (실전투자 모드)")
        kis = initialize_kis(args.secret, args.virtual)
        account = kis.account()
        balance = account.balance()
        total_investment = int(balance.total)
        logger.info(f"현재 총평가금액: {total_investment:,}원")
    else:
        # 지정된 투자액 사용
        total_investment = args.investment
        mode_str = "모의투자" if args.virtual else "실전투자"
        logger.info(f"\n투자액 설정: 수동 지정 ({total_investment:,}원) - {mode_str} 모드")

    # 매수 수량 계산
    df_buy = calculate_quantities(portfolio_file, total_investment)

    # 결과 출력
    logger.info("\n" + "=" * 80)
    logger.info("매수 계획")
    logger.info("=" * 80)
    logger.info(f"\n{'종목코드':<10} {'종목명':<20} {'가격':>12} {'수량':>8} {'투자액':>15} {'실투자액':>15}")
    logger.info("-" * 80)

    for _, row in df_buy.iterrows():
        logger.info(f"{row['code']:<10} {row['종목명']:<20} {row['end_price']:>12,.0f} {row['매수수량']:>8} "
              f"{row['투자액']:>15,.0f} {row['실투자액']:>15,.0f}")

    logger.info("-" * 80)
    total_actual = df_buy['실투자액'].sum()
    remaining = total_investment - total_actual
    logger.info(f"{'합계':<32} {'':<12} {'':<8} {total_investment:>15,} {total_actual:>15,}")
    logger.info(f"{'잔액':<32} {'':<12} {'':<8} {'':<15} {remaining:>15,}")

    # 결과 저장
    base_name = portfolio_file.stem  # portfolio_2025-10-02
    date_str = base_name.split('_')[1]  # 2025-10-02

    output_file = portfolio_file.parent / f"buy_plan_{date_str}.csv"
    df_buy.to_csv(output_file, index=False, encoding='utf-8-sig')
    logger.info(f"\n매수 계획이 {output_file}에 저장되었습니다.")

    # 실행 옵션이 있을 경우 실제 주문 실행
    if args.execute:
        # PyKis 초기화 (아직 초기화되지 않은 경우만)
        if kis is None:
            kis = initialize_kis(args.secret, args.virtual)

        # 매수 주문 실행 (모의투자 여부 전달)
        results = execute_buy_orders(kis, df_buy, is_virtual=bool(args.virtual))

        # 결과 저장
        results_df = pd.DataFrame(results)
        results_file = portfolio_file.parent / f"buy_results_{date_str}.csv"
        results_df.to_csv(results_file, index=False, encoding='utf-8-sig')
        logger.info(f"\n주문 결과가 {results_file}에 저장되었습니다.")

        # 실행 성공 여부 판단 (매수/매도 실패가 없으면 성공)
        buy_failed_count = sum(1 for r in results if r['status'] == 'buy_failed')
        sell_failed_count = sum(1 for r in results if r['status'] == 'sell_failed')
        liquidate_failed_count = sum(1 for r in results if r['status'] == 'liquidate_failed')
        execution_success = (buy_failed_count == 0 and sell_failed_count == 0 and liquidate_failed_count == 0)

        # 실행 기록 저장
        record_execution(portfolio_file.name, execution_success)

        if execution_success:
            logger.info("\n🎉 리밸런싱 성공!")
        else:
            logger.info("\n⚠️  일부 주문이 실패했습니다. 결과를 확인하세요.")
    else:
        logger.info("\n💡 매수 주문을 실행하려면 --execute 옵션을 사용하세요.")
        logger.info(f"   예: python buy_portfolio.py --secret secret.json --execute")


if __name__ == "__main__":
    main()