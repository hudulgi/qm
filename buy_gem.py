"""
GEM(Global Equities Momentum) 전략 실행
12개월 토탈리턴이 가장 높은 종목에 전액 투자하는 모멘텀 전략

Created on 2025-11-14
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pykis import PyKis, KisAuth, KisQuote


# 투자 설정
MAX_RETRIES = 3  # 최대 재시도 횟수
RETRY_DELAY = 1  # 재시도 간 대기 시간 (초)
ORDER_DELAY = 0.5  # 주문 간 대기 시간 (초)
REBALANCE_WAIT_TIME = 60  # 리밸런싱 매도 후 매수 대기 시간 (초)
EXECUTION_LOG_FILE = "gem_execution_log.json"  # 실행 기록 파일
BUFFER_RATIO = 0.99  # 매수 시 투자액 버퍼 비율 (99%, 1% 여유)
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

    # 로그 파일명: gem_YYYYMMDD_HHMMSS.log
    log_filename = os.path.join(
        LOG_DIR,
        f"gem_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    # 로거 생성
    logger = logging.getLogger('GEM')
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
            logger.warning(f"실행 기록 파일 로드 실패: {e}")
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
        logger.warning(f"실행 기록 저장 실패: {e}")


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
            logger.warning(f"⚠️  이번 달({current_month})에 이미 실행되었습니다.")
            logger.info(f"   실행일: {execution.get('date')}")
            logger.info(f"   선택 종목: {execution.get('selected_code')} ({execution.get('selected_name')})")
            return True

    logger.info(f"✅ 이번 달({current_month}) 첫 실행입니다.")
    return False


def record_execution(selected_code, selected_name, success):
    """
    실행 기록 추가

    Args:
        selected_code: 선택된 종목 코드
        selected_name: 선택된 종목명
        success: 실행 성공 여부
    """
    log_data = load_execution_log()

    execution_record = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "month": datetime.now().strftime("%Y-%m"),
        "selected_code": selected_code,
        "selected_name": selected_name,
        "success": success,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    log_data["executions"].append(execution_record)
    save_execution_log(log_data)


def get_single_nav(kis: PyKis, stock_code: str, date: str, logger=None) -> float:
    """
    특정 날짜의 NAV 값 조회 (재시도 로직 포함)

    Args:
        kis: PyKis 인스턴스
        stock_code: 종목코드
        date: 조회 날짜 (YYYYMMDD)
        logger: 로거 인스턴스

    Returns:
        float: NAV 값
    """
    path = "/uapi/etfetn/v1/quotations/nav-comparison-daily-trend"

    headers = {
        "tr_id": "FHPST02440200"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_DATE_1": date,
        "FID_INPUT_DATE_2": date
    }

    # 재시도 로직
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = kis.request(
                path=path,
                method="GET",
                params=params,
                headers=headers,
                domain="real"
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('rt_cd') == '0':
                    output = data.get('output', [])
                    if output:
                        nav_value = float(output[0]['nav'])
                        if nav_value <= 0:
                            if logger:
                                logger.debug(f"{stock_code} {date}: NAV={nav_value} (유효하지 않음, 주변일 탐색 필요)")
                            return None
                        if logger:
                            logger.debug(f"{stock_code} {date}: NAV={nav_value}")
                        return nav_value
                    else:
                        if logger:
                            logger.debug(f"{stock_code} {date}: output 배열이 비어있음 (휴장일 가능성)")

            return None

        except Exception as e:
            error_msg = str(e).lower()
            # 네트워크 관련 오류 체크
            network_errors = ['connection', 'timeout', 'remote', 'disconnect']
            is_network_error = any(keyword in error_msg for keyword in network_errors)

            if is_network_error and attempt < MAX_RETRIES:
                if logger:
                    logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}] NAV 조회 오류 ({stock_code}, {date}): {e}")
                time.sleep(RETRY_DELAY * attempt)
            else:
                if attempt == MAX_RETRIES:
                    if logger:
                        logger.error(f"NAV 조회 최대 재시도 초과 ({stock_code}, {date}): {e}")
                return None

    return None


def get_dividends(kis: PyKis, stock_code: str, start_date: str, end_date: str) -> float:
    """
    배당금 정보 조회 (재시도 로직 포함)

    Args:
        kis: PyKis 인스턴스
        stock_code: 종목코드
        start_date: 조회 시작일 (YYYYMMDD)
        end_date: 조회 종료일 (YYYYMMDD)

    Returns:
        float: 총 배당금
    """
    path = "/uapi/domestic-stock/v1/ksdinfo/dividend"

    headers = {
        "tr_id": "HHKDB669102C0"
    }

    params = {
        "CTS": "",
        "GB1": "0",
        "F_DT": start_date,
        "T_DT": end_date,
        "SHT_CD": stock_code,
        "HIGH_GB": ""
    }

    # 재시도 로직
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = kis.request(
                path=path,
                method="GET",
                params=params,
                headers=headers,
                domain="real"
            )

            total_dividend = 0.0

            if response.status_code == 200:
                data = response.json()
                if data.get('rt_cd') == '0':
                    output = data.get('output1', [])
                    if output:
                        for div in output:
                            if 'per_sto_divi_amt' in div:
                                total_dividend += float(div['per_sto_divi_amt'])

            return total_dividend

        except Exception as e:
            error_msg = str(e).lower()
            # 네트워크 관련 오류 체크
            network_errors = ['connection', 'timeout', 'remote', 'disconnect']
            is_network_error = any(keyword in error_msg for keyword in network_errors)

            if is_network_error and attempt < MAX_RETRIES:
                logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}] 배당금 조회 오류 ({stock_code}): {e}")
                time.sleep(RETRY_DELAY * attempt)
            else:
                if attempt == MAX_RETRIES:
                    logger.error(f"배당금 조회 최대 재시도 초과 ({stock_code}): {e}")
                return 0.0

    return 0.0


def calculate_12m_total_return(kis: PyKis, stock_code: str, stock_name: str = None, logger=None) -> dict:
    """
    12개월 토탈리턴 수익률 계산 (NAV 가격 변동 + 배당)

    Args:
        kis: PyKis 인스턴스
        stock_code: 종목코드
        stock_name: 종목명 (옵션)
        logger: 로거 인스턴스

    Returns:
        dict: 토탈리턴 정보
    """
    # 현재 날짜
    today = datetime.now()
    end_date = today.strftime("%Y%m%d")

    # 12개월 전 날짜
    start_date = (today - timedelta(days=365)).strftime("%Y%m%d")

    # 1. 시작일 NAV 조회
    nav_start = get_single_nav(kis, stock_code, start_date, logger)

    if nav_start is None:
        # 영업일이 아닐 수 있으므로 며칠 앞뒤로 시도
        for offset in range(1, 10):
            adjusted_date = (today - timedelta(days=365+offset)).strftime("%Y%m%d")
            nav_start = get_single_nav(kis, stock_code, adjusted_date, logger)
            if nav_start is not None:
                start_date = adjusted_date
                break

    # 2. 현재 NAV 조회
    nav_end = get_single_nav(kis, stock_code, end_date, logger)

    if nav_end is None:
        # 오늘이 영업일이 아닐 수 있으므로 최근 영업일 찾기
        for offset in range(1, 10):
            adjusted_date = (today - timedelta(days=offset)).strftime("%Y%m%d")
            nav_end = get_single_nav(kis, stock_code, adjusted_date, logger)
            if nav_end is not None:
                end_date = adjusted_date
                break

    if nav_start is None or nav_end is None:
        msg = f"❌ {stock_code} ({stock_name}): NAV 조회 실패"
        if logger:
            logger.error(msg)
        else:
            print(msg)
        return None

    # NAV 값이 0인 경우도 체크
    if nav_start <= 0 or nav_end <= 0:
        msg = f"❌ {stock_code} ({stock_name}): NAV 값이 유효하지 않음 (시작: {nav_start}, 종료: {nav_end})"
        if logger:
            logger.error(msg)
        else:
            print(msg)
        return None

    # 3. 배당금 조회
    total_dividend = get_dividends(kis, stock_code, start_date, end_date)

    # 4. 수익률 계산
    price_return = ((nav_end - nav_start) / nav_start) * 100
    dividend_yield = (total_dividend / nav_start) * 100
    total_return = ((nav_end + total_dividend - nav_start) / nav_start) * 100

    result = {
        "stock_code": stock_code,
        "stock_name": stock_name or stock_code,
        "start_date": start_date,
        "end_date": end_date,
        "nav_start": nav_start,
        "nav_end": nav_end,
        "total_dividend": total_dividend,
        "price_return": price_return,
        "dividend_yield": dividend_yield,
        "total_return": total_return
    }

    return result


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
                code = getattr(stock, 'symbol', getattr(stock, 'code', None))
                if code:
                    code = str(code).zfill(6)
                    qty = int(stock.qty)
                    name = getattr(stock, 'name', '(이름없음)')
                    if qty > 0:
                        holdings[code] = {'qty': qty, 'name': name}

        return holdings

    except Exception as e:
        logger.warning(f"보유 잔고 조회 실패: {e}")
        return {}


def get_stock_name(kis, stock_code):
    """
    종목명 조회 (재시도 로직 포함)

    Args:
        kis: PyKis 객체
        stock_code: 종목코드

    Returns:
        str: 종목명
    """
    # 재시도 로직
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            stock = kis.stock(stock_code)
            return stock.name
        except Exception as e:
            error_msg = str(e).lower()
            # 네트워크 관련 오류 체크
            network_errors = ['connection', 'timeout', 'remote', 'disconnect']
            is_network_error = any(keyword in error_msg for keyword in network_errors)

            if is_network_error and attempt < MAX_RETRIES:
                logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}] 종목명 조회 오류 ({stock_code}): {e}")
                time.sleep(RETRY_DELAY * attempt)
            else:
                if attempt == MAX_RETRIES:
                    logger.error(f"종목명 조회 최대 재시도 초과 ({stock_code}): {e}")
                else:
                    logger.warning(f"{stock_code} 종목명 조회 실패: {e}")
                return stock_code  # 실패시 종목코드 반환

    return stock_code


def get_current_price(kis, stock_code):
    """
    현재가 조회 (재시도 로직 포함)

    Args:
        kis: PyKis 객체
        stock_code: 종목코드

    Returns:
        int: 현재가
    """
    # 재시도 로직
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            stock = kis.stock(stock_code)
            price_data: KisQuote = stock.quote()
            return int(price_data.close)
        except Exception as e:
            error_msg = str(e).lower()
            # 네트워크 관련 오류 체크
            network_errors = ['connection', 'timeout', 'remote', 'disconnect']
            is_network_error = any(keyword in error_msg for keyword in network_errors)

            if is_network_error and attempt < MAX_RETRIES:
                logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}] 현재가 조회 오류 ({stock_code}): {e}")
                time.sleep(RETRY_DELAY * attempt)
            else:
                if attempt == MAX_RETRIES:
                    logger.error(f"현재가 조회 최대 재시도 초과 ({stock_code}): {e}")
                else:
                    logger.warning(f"{stock_code} 현재가 조회 실패: {e}")
                return None

    return None


def get_total_balance(kis):
    """
    총평가금액 조회 (재시도 로직 포함)

    Args:
        kis: PyKis 객체

    Returns:
        int: 총평가금액
    """
    # 재시도 로직
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            account = kis.account()
            balance = account.balance()
            return int(balance.total)
        except Exception as e:
            error_msg = str(e).lower()
            # 네트워크 관련 오류 체크
            network_errors = ['connection', 'timeout', 'remote', 'disconnect']
            is_network_error = any(keyword in error_msg for keyword in network_errors)

            if is_network_error and attempt < MAX_RETRIES:
                logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}] 총평가금액 조회 오류: {e}")
                time.sleep(RETRY_DELAY * attempt)
            else:
                if attempt == MAX_RETRIES:
                    logger.error(f"총평가금액 조회 최대 재시도 초과: {e}")
                else:
                    logger.warning(f"총평가금액 조회 실패: {e}")
                return None

    return None


def execute_rebalancing(kis, target_code, target_name, total_investment, is_virtual=False):
    """
    리밸런싱 실행: 기존 종목 전량 매도 후 목표 종목 전량 매수

    Args:
        kis: PyKis 객체
        target_code: 목표 종목코드
        target_name: 목표 종목명
        total_investment: 총 투자액
        is_virtual: 모의투자 여부

    Returns:
        dict: 실행 결과
    """
    results = {
        'sell_orders': [],
        'buy_order': None,
        'success': False
    }

    # 1. 현재 보유 종목 조회
    holdings = get_current_holdings(kis)

    logger.info(f"\n현재 보유 종목: {len(holdings)}개")
    for code, info in holdings.items():
        logger.info(f"  {code} ({info['name']}): {info['qty']}주")

    # 2. 목표 종목 이외의 모든 종목 매도
    non_target_holdings = {code: info for code, info in holdings.items() if code != target_code}

    if non_target_holdings:
        logger.info(f"\n{'='*80}")
        logger.info(f"[1단계] 기존 보유 종목 전량 매도 ({len(non_target_holdings)}개)")
        logger.info(f"{'='*80}")

        for code, info in non_target_holdings.items():
            qty = info['qty']
            logger.info(f"\n[매도] {code} ({info['name']}): {qty}주")

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    if attempt > 1:
                        logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}]")
                        time.sleep(RETRY_DELAY * (attempt - 1))

                    # 시장가 전량 매도
                    sell_order = kis.stock(code).sell(price=None, qty=qty, condition=None, execution=None)
                    logger.info(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                    results['sell_orders'].append({
                        'code': code,
                        'name': info['name'],
                        'qty': qty,
                        'status': 'success',
                        'order': sell_order
                    })
                    break

                except Exception as e:
                    error_msg = str(e).lower()
                    no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']

                    if any(keyword in error_msg for keyword in no_retry_keywords):
                        logger.error(f"[매도 실패] {e} (재시도 불가)")
                        results['sell_orders'].append({
                            'code': code,
                            'name': info['name'],
                            'qty': qty,
                            'status': 'failed',
                            'error': str(e)
                        })
                        break

                    if attempt == MAX_RETRIES:
                        logger.error(f"[매도 실패] {e} (최대 재시도 초과)")
                        results['sell_orders'].append({
                            'code': code,
                            'name': info['name'],
                            'qty': qty,
                            'status': 'failed',
                            'error': str(e)
                        })

            time.sleep(ORDER_DELAY)

        # 매도 후 대기
        if results['sell_orders']:
            logger.info(f"\n[대기] 매도 완료 후 {REBALANCE_WAIT_TIME}초 대기...")
            time.sleep(REBALANCE_WAIT_TIME)

    # 3. 목표 종목이 이미 보유 중인지 확인
    target_holding = holdings.get(target_code, {}).get('qty', 0)

    if target_holding > 0:
        logger.info(f"\n[알림] 목표 종목 {target_code} ({target_name})을 이미 {target_holding}주 보유 중입니다.")
        logger.info(f"[알림] 기존 보유 종목을 유지합니다.")
        results['success'] = True
        return results

    # 4. 목표 종목 매수
    logger.info(f"\n{'='*80}")
    logger.info(f"[2단계] 목표 종목 전액 매수")
    logger.info(f"{'='*80}")

    # 현재가 조회
    current_price = get_current_price(kis, target_code)

    if current_price is None:
        logger.error(f"❌ 현재가 조회 실패: {target_code}")
        return results

    # 매수 수량 계산 (버퍼 적용으로 가격 변동 대비)
    safe_investment = int(total_investment * BUFFER_RATIO)
    buy_qty = int(safe_investment / current_price)

    if buy_qty <= 0:
        logger.error(f"❌ 매수 수량이 0입니다. 투자액을 확인하세요.")
        return results

    logger.info(f"\n[매수] {target_code} ({target_name})")
    logger.info(f"  현재가: {current_price:,}원")
    logger.info(f"  총투자액: {total_investment:,}원")
    logger.info(f"  실투자액: {safe_investment:,}원 (버퍼 {int((1-BUFFER_RATIO)*100)}% 적용)")
    logger.info(f"  매수수량: {buy_qty}주")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}]")
                time.sleep(RETRY_DELAY * (attempt - 1))
            else:
                logger.info(f"[매수] 지정가, 수량={buy_qty}주, 주문가격={current_price:,}원")

            # 지정가 매수 주문
            buy_order = kis.stock(target_code).buy(price=current_price, qty=buy_qty, condition=None, execution=None)

            logger.info(f"[매수 성공] 주문번호: {buy_order.number if hasattr(buy_order, 'number') else 'N/A'}")
            results['buy_order'] = {
                'code': target_code,
                'name': target_name,
                'qty': buy_qty,
                'price': current_price,
                'status': 'success',
                'order': buy_order
            }
            results['success'] = True
            break

        except Exception as e:
            error_msg = str(e).lower()
            no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']

            if any(keyword in error_msg for keyword in no_retry_keywords):
                logger.error(f"[매수 실패] {e} (재시도 불가)")
                results['buy_order'] = {
                    'code': target_code,
                    'name': target_name,
                    'qty': buy_qty,
                    'status': 'failed',
                    'error': str(e)
                }
                break

            if attempt == MAX_RETRIES:
                logger.error(f"[매수 실패] {e} (최대 재시도 초과)")
                results['buy_order'] = {
                    'code': target_code,
                    'name': target_name,
                    'qty': buy_qty,
                    'status': 'failed',
                    'error': str(e)
                }

    return results


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='GEM(Global Equities Momentum) 전략 실행')
    parser.add_argument('--execute', action='store_true', help='실제 주문 실행 (기본: 분석만 수행)')
    parser.add_argument('--secret', required=True, help='실전 계좌 secret 파일 경로 (필수)')
    parser.add_argument('--virtual', default=None, help='모의투자 계좌 secret 파일 경로 (옵션)')
    parser.add_argument('--investment', type=int, default=None, help='총 투자액 (원 단위, 기본: 현재 총평가금액 사용)')
    parser.add_argument('--force', action='store_true', help='이번 달 실행 기록 무시하고 강제 실행')
    args = parser.parse_args()

    # 로거 초기화
    setup_logger()
    logger.info("="*80)
    logger.info("GEM(Global Equities Momentum) 전략 시작")
    logger.info("="*80)

    # 실행 기록 확인 (--execute 모드이고 --force가 아닐 때만)
    if args.execute and not args.force:
        if check_monthly_execution():
            logger.warning("이미 실행되었으므로 종료합니다.")
            logger.info("강제 실행하려면 --force 옵션을 사용하세요.")
            return

    # PyKis 초기화
    logger.info("🔐 인증 중...")
    kis = initialize_kis(args.secret, args.virtual)
    logger.info("인증 완료")

    # 대상 종목 코드 (수동 지정, 나중에 파라미터로 받도록 수정 가능)
    target_codes = ["069500", "379800", "423160"]

    # 종목명 조회
    logger.info("="*80)
    logger.info("종목명 조회 중...")
    logger.info("="*80)

    target_stocks = []
    for code in target_codes:
        name = get_stock_name(kis, code)
        target_stocks.append({"code": code, "name": name})
        logger.info(f"  - {code}: {name}")
        time.sleep(0.3)  # API 호출 제한 고려

    logger.info("="*80)
    logger.info("📊 GEM 전략 - 12개월 토탈리턴 분석")
    logger.info("="*80)
    logger.info(f"분석 종목: {len(target_stocks)}개")

    # 각 종목의 12개월 토탈리턴 계산
    results = []

    for stock in target_stocks:
        logger.info("-"*80)
        logger.info(f"종목 분석: {stock['code']} ({stock['name']})")
        logger.info("-"*80)

        result = calculate_12m_total_return(kis, stock['code'], stock['name'], logger)

        if result:
            results.append(result)
            print(f"✅ 12개월 토탈리턴: {result['total_return']:.2f}%")
            print(f"   가격 수익률: {result['price_return']:.2f}%")
            print(f"   배당 수익률: {result['dividend_yield']:.2f}%")
        else:
            print(f"❌ 분석 실패")

        time.sleep(0.5)

    if not results:
        print("\n❌ 모든 종목 분석 실패")
        return

    # 결과 요약 및 최고 수익률 종목 선택
    print(f"\n{'='*80}")
    print(f"📈 분석 결과 요약")
    print(f"{'='*80}")

    # 토탈리턴 순으로 정렬
    results.sort(key=lambda x: x['total_return'], reverse=True)

    print(f"\n{'순위':<5} {'종목코드':<10} {'종목명':<30} {'12개월 토탈리턴':>15}")
    print(f"{'-'*80}")

    for idx, result in enumerate(results, 1):
        marker = "🥇" if idx == 1 else "  "
        print(f"{marker} {idx:<3} {result['stock_code']:<10} {result['stock_name']:<30} {result['total_return']:>14.2f}%")

    # 최고 수익률 종목 선택
    best_stock = results[0]

    print(f"\n{'='*80}")
    print(f"🎯 선택 종목: {best_stock['stock_code']} ({best_stock['stock_name']})")
    print(f"   12개월 토탈리턴: {best_stock['total_return']:.2f}%")
    print(f"{'='*80}")

    # 실행 모드
    if args.execute:
        # 투자액 결정
        if args.investment is None:
            if args.virtual:
                logger.error("\n[오류] 모의투자 모드에서는 --investment 옵션으로 투자액을 지정해야 합니다.")
                logger.info("예: python buy_gem.py --execute --secret secret.json --virtual secret_virtual.json --investment 10000000")
                return

            # 실전투자 모드에서 총평가금액 조회
            logger.info("\n투자액 설정: 현재 총평가금액 사용 (실전투자 모드)")
            total_investment = get_total_balance(kis)

            if total_investment is None:
                logger.error("\n❌ 총평가금액 조회 실패. 프로그램을 종료합니다.")
                return

            logger.info(f"현재 총평가금액: {total_investment:,}원")
        else:
            total_investment = args.investment
            mode_str = "모의투자" if args.virtual else "실전투자"
            logger.info(f"\n투자액 설정: 수동 지정 ({total_investment:,}원) - {mode_str} 모드")

        # 리밸런싱 실행
        logger.info(f"\n{'='*80}")
        logger.info(f"⚙️  리밸런싱 실행")
        logger.info(f"{'='*80}")

        rebalance_results = execute_rebalancing(
            kis=kis,
            target_code=best_stock['stock_code'],
            target_name=best_stock['stock_name'],
            total_investment=total_investment,
            is_virtual=bool(args.virtual)
        )

        # 결과 출력
        logger.info(f"\n{'='*80}")
        logger.info(f"✅ 리밸런싱 완료")
        logger.info(f"{'='*80}")

        if rebalance_results['sell_orders']:
            logger.info(f"\n매도 주문: {len(rebalance_results['sell_orders'])}건")
            for order in rebalance_results['sell_orders']:
                status_mark = "✅" if order['status'] == 'success' else "❌"
                logger.info(f"  {status_mark} {order['code']} ({order['name']}): {order['qty']}주")

        if rebalance_results['buy_order']:
            buy_order = rebalance_results['buy_order']
            status_mark = "✅" if buy_order['status'] == 'success' else "❌"
            logger.info(f"\n매수 주문:")
            logger.info(f"  {status_mark} {buy_order['code']} ({buy_order['name']}): {buy_order['qty']}주")

        if rebalance_results['success']:
            logger.info(f"\n🎉 리밸런싱 성공!")
            # 실행 기록 저장
            record_execution(
                selected_code=best_stock['stock_code'],
                selected_name=best_stock['stock_name'],
                success=True
            )
        else:
            logger.warning(f"\n⚠️  일부 주문이 실패했습니다. 결과를 확인하세요.")
            # 실패도 기록 (성공하지 않음으로 표시)
            record_execution(
                selected_code=best_stock['stock_code'],
                selected_name=best_stock['stock_name'],
                success=False
            )

    else:
        print("\n💡 실제 주문을 실행하려면 --execute 옵션을 사용하세요.")
        print(f"   예: python buy_gem.py --execute --secret secret.json --investment 10000000")


if __name__ == "__main__":
    main()