import pandas as pd
from pathlib import Path
import os
import json
import argparse
import time
from pykis import PyKis

# 투자 설정
TOTAL_INVESTMENT = 20_000_000 + 17_330  # 총 투자액
MAX_RETRIES = 3  # 최대 재시도 횟수
RETRY_DELAY = 1  # 재시도 간 대기 시간 (초)
ORDER_DELAY = 0.5  # 주문 간 대기 시간 (초)
REBALANCE_WAIT_TIME = 60  # 리밸런싱 매도 후 매수 대기 시간 (초)


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

    print(f"최신 포트폴리오 파일: {latest_file.name}")
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

    print(f"\n총 투자액: {total_investment:,}원")
    print(f"종목 수: {num_stocks}개")
    print(f"종목당 투자액: {amount_per_stock:,.0f}원")

    # 수량 계산
    df['투자액'] = amount_per_stock
    df['매수수량'] = (df['투자액'] / df['end_price']).astype(int)
    df['실투자액'] = df['매수수량'] * df['end_price']

    # 컬럼 순서 재정렬
    cols = ['code', '종목명', 'end_price', '매수수량', '투자액', '실투자액',
            'adjusted_momentum_12m', 'fip', 'end_price_date']
    df = df[cols]

    return df


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
        print(f"모의투자 모드로 초기화: {secret_file}, {virtual_file}")
        return PyKis(secret_file, virtual_file, keep_token=True)
    else:
        print(f"실전투자 모드로 초기화: {secret_file}")
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

        print(f"\n현재 보유 종목 수: {len(holdings)}개")
        if holdings:
            print("보유 종목 목록:")
            for code, info in holdings.items():
                print(f"  {code} ({info['name']}): {info['qty']}주")

        return holdings

    except Exception as e:
        print(f"[경고] 보유 잔고 조회 실패: {e}")
        print("보유 종목이 없다고 가정하고 진행합니다.")
        return {}


def execute_buy_orders(kis, df_buy):
    """
    계산된 수량으로 매수 주문 실행 (리밸런싱 포함, 최우선 지정가, 재시도 로직 포함)

    리밸런싱 로직:
    1. 보유량 > 목표량: (보유량 - 목표량)만큼 시장가 매도
    2. 보유량 < 목표량: (목표량 - 보유량)만큼 최우선 지정가 매수
    3. 보유량 = 목표량: 아무 작업도 하지 않음

    Args:
        kis: PyKis 객체
        df_buy: 매수 계획이 담긴 DataFrame

    Returns:
        list: 주문 결과 리스트
    """
    results = []

    # 현재 보유 종목 조회
    holdings = get_current_holdings(kis)

    # 매수 예정 종목 코드 set
    target_codes = set(df_buy['code'].tolist())

    print("\n" + "=" * 80)
    print("매수 주문 실행 (리밸런싱 포함)")
    print("=" * 80)

    # 1단계: 매수 예정에 없는 보유 종목 전량 매도
    non_target_holdings = {code: info for code, info in holdings.items() if code not in target_codes}

    if non_target_holdings:
        print(f"\n[전량 매도] 매수 예정에 없는 보유 종목 {len(non_target_holdings)}개를 매도합니다.")

        for code, info in non_target_holdings.items():
            qty = info['qty']
            print(f"\n[전량 매도] {code}: {qty}주 매도")

            # 시장가 매도 재시도 로직
            sell_success = False
            sell_error = None

            for sell_attempt in range(1, MAX_RETRIES + 1):
                try:
                    if sell_attempt > 1:
                        print(f"[매도 재시도 {sell_attempt}/{MAX_RETRIES}] {code}")
                        time.sleep(RETRY_DELAY * (sell_attempt - 1))

                    # 시장가 전량 매도
                    sell_order = kis.stock(code).sell(price=None, qty=qty, condition=None, execution=None)

                    print(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                    sell_success = True
                    break

                except Exception as e:
                    sell_error = str(e)
                    error_msg = sell_error.lower()

                    # 재시도 불가능한 오류 체크
                    no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                    if any(keyword in error_msg for keyword in no_retry_keywords):
                        print(f"[매도 실패] {code}: {sell_error} (재시도 불가)")
                        break

                    if sell_attempt < MAX_RETRIES:
                        print(f"[매도 오류] {code}: {sell_error} (재시도 예정)")
                    else:
                        print(f"[매도 실패] {code}: {sell_error} (최대 재시도 횟수 초과)")

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
            print(f"\n[대기] 전량 매도 완료 후 {REBALANCE_WAIT_TIME}초 대기...")
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
                print(f"[리밸런싱 매도] {code} {name}: 현재 {current_qty}주 → 목표 0주 (전량 매도)")

                # 시장가 매도 재시도 로직
                sell_success = False
                sell_error = None

                for sell_attempt in range(1, MAX_RETRIES + 1):
                    try:
                        if sell_attempt > 1:
                            print(f"[매도 재시도 {sell_attempt}/{MAX_RETRIES}] {code} {name}")
                            time.sleep(RETRY_DELAY * (sell_attempt - 1))

                        # 시장가 전량 매도
                        sell_order = kis.stock(code).sell(price=None, qty=current_qty, condition=None, execution=None)

                        print(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                        sell_success = True
                        break

                    except Exception as e:
                        sell_error = str(e)
                        error_msg = sell_error.lower()

                        # 재시도 불가능한 오류 체크
                        no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                        if any(keyword in error_msg for keyword in no_retry_keywords):
                            print(f"[매도 실패] {code} {name}: {sell_error} (재시도 불가)")
                            break

                        if sell_attempt < MAX_RETRIES:
                            print(f"[매도 오류] {code} {name}: {sell_error} (재시도 예정)")
                        else:
                            print(f"[매도 실패] {code} {name}: {sell_error} (최대 재시도 횟수 초과)")

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
                    print(f"[대기] {REBALANCE_WAIT_TIME}초 대기...")
                    time.sleep(REBALANCE_WAIT_TIME)
            else:
                # 보유량도 없으면 스킵
                print(f"[SKIP] {code} {name}: 목표 0, 보유 0 (변동 없음)")
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
            print(f"[유지] {code} {name}: 현재 {current_qty}주 보유, 목표 {target_qty}주 (변동 없음)")
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
            print(f"\n[리밸런싱 매도] {code} {name}: 현재 {current_qty}주 → 목표 {target_qty}주 ({sell_qty}주 매도)")

            # 시장가 매도 재시도 로직
            sell_success = False
            sell_error = None

            for sell_attempt in range(1, MAX_RETRIES + 1):
                try:
                    if sell_attempt > 1:
                        print(f"[매도 재시도 {sell_attempt}/{MAX_RETRIES}] {code} {name}")
                        time.sleep(RETRY_DELAY * (sell_attempt - 1))

                    # 시장가 매도
                    sell_order = kis.stock(code).sell(price=None, qty=sell_qty, condition=None, execution=None)

                    print(f"[매도 성공] 주문번호: {sell_order.number if hasattr(sell_order, 'number') else 'N/A'}")
                    sell_success = True
                    break

                except Exception as e:
                    sell_error = str(e)
                    error_msg = sell_error.lower()

                    # 재시도 불가능한 오류 체크
                    no_retry_keywords = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
                    if any(keyword in error_msg for keyword in no_retry_keywords):
                        print(f"[매도 실패] {code} {name}: {sell_error} (재시도 불가)")
                        break

                    if sell_attempt < MAX_RETRIES:
                        print(f"[매도 오류] {code} {name}: {sell_error} (재시도 예정)")
                    else:
                        print(f"[매도 실패] {code} {name}: {sell_error} (최대 재시도 횟수 초과)")

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
                print(f"[대기] {REBALANCE_WAIT_TIME}초 대기...")
                time.sleep(REBALANCE_WAIT_TIME)

        else:
            # 보유량 < 목표량 → 매수 필요
            buy_qty = delta
            print(f"\n[리밸런싱 매수] {code} {name}: 현재 {current_qty}주 → 목표 {target_qty}주 ({buy_qty}주 매수)")

            # 매수 주문 재시도 로직
            buy_success = False
            last_error = None
            attempt = 0

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    if attempt > 1:
                        print(f"[재시도 {attempt}/{MAX_RETRIES}] {code} {name}")
                        time.sleep(RETRY_DELAY * (attempt - 1))  # 지수 백오프
                    else:
                        print(f"[매수] {code} {name}: 가격={price:,}원, 수량={buy_qty}주")

                    # 최우선 지정가 매수 주문
                    order = kis.stock(code).buy(price=price, qty=buy_qty, condition='best', execution=None)

                    print(f"[성공] 주문번호: {order.number if hasattr(order, 'number') else 'N/A'}")
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
                        print(f"[실패] {code} {name}: {last_error} (재시도 불가)")
                        break

                    # 마지막 시도가 아니면 재시도
                    if attempt < MAX_RETRIES:
                        print(f"[오류] {code} {name}: {last_error} (재시도 예정)")
                    else:
                        print(f"[실패] {code} {name}: {last_error} (최대 재시도 횟수 초과)")

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

    print("=" * 80)

    # 결과 요약
    buy_success_count = sum(1 for r in results if r['status'] == 'buy_success')
    sell_success_count = sum(1 for r in results if r['status'] == 'sell_success')
    liquidated_count = sum(1 for r in results if r['status'] == 'liquidated')
    buy_failed_count = sum(1 for r in results if r['status'] == 'buy_failed')
    sell_failed_count = sum(1 for r in results if r['status'] == 'sell_failed')
    liquidate_failed_count = sum(1 for r in results if r['status'] == 'liquidate_failed')
    unchanged_count = sum(1 for r in results if r['status'] == 'unchanged')
    skipped_count = sum(1 for r in results if r['status'] == 'skipped')

    print(f"\n주문 결과 요약:")
    if liquidated_count > 0:
        print(f"  전량 매도: {liquidated_count}건 (매수예정외 종목)")
    print(f"  매수 성공: {buy_success_count}건")
    print(f"  매도 성공: {sell_success_count}건 (리밸런싱)")
    print(f"  수량 유지: {unchanged_count}건")
    if buy_failed_count > 0:
        print(f"  매수 실패: {buy_failed_count}건")
    if sell_failed_count > 0:
        print(f"  매도 실패: {sell_failed_count}건")
    if liquidate_failed_count > 0:
        print(f"  전량 매도 실패: {liquidate_failed_count}건")
    if skipped_count > 0:
        print(f"  건너뜀: {skipped_count}건")

    # 재시도 통계
    retry_count = sum(1 for r in results if r.get('attempts', 1) > 1)
    if retry_count > 0:
        print(f"\n재시도 성공: {retry_count}건")

    return results


def main():
    # 명령줄 인수 파싱
    parser = argparse.ArgumentParser(description='포트폴리오 매수 계획 생성 및 주문 실행')
    parser.add_argument('--execute', action='store_true', help='실제 매수 주문 실행 (기본: 계획만 출력)')
    parser.add_argument('--secret', default='secret.json', help='실전 계좌 secret 파일 경로 (기본: secret.json)')
    parser.add_argument('--virtual', default=None, help='모의투자 계좌 secret 파일 경로 (옵션)')
    args = parser.parse_args()

    # 최신 포트폴리오 파일 찾기
    portfolio_file = get_latest_portfolio_file()

    # 매수 수량 계산
    df_buy = calculate_quantities(portfolio_file, TOTAL_INVESTMENT)

    # 결과 출력
    print("\n" + "=" * 80)
    print("매수 계획")
    print("=" * 80)
    print(f"\n{'종목코드':<10} {'종목명':<20} {'가격':>12} {'수량':>8} {'투자액':>15} {'실투자액':>15}")
    print("-" * 80)

    for _, row in df_buy.iterrows():
        print(f"{row['code']:<10} {row['종목명']:<20} {row['end_price']:>12,.0f} {row['매수수량']:>8} "
              f"{row['투자액']:>15,.0f} {row['실투자액']:>15,.0f}")

    print("-" * 80)
    total_actual = df_buy['실투자액'].sum()
    remaining = TOTAL_INVESTMENT - total_actual
    print(f"{'합계':<32} {'':<12} {'':<8} {TOTAL_INVESTMENT:>15,} {total_actual:>15,}")
    print(f"{'잔액':<32} {'':<12} {'':<8} {'':<15} {remaining:>15,}")

    # 결과 저장
    base_name = portfolio_file.stem  # portfolio_2025-10-02
    date_str = base_name.split('_')[1]  # 2025-10-02

    output_file = portfolio_file.parent / f"buy_plan_{date_str}.csv"
    df_buy.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n매수 계획이 {output_file}에 저장되었습니다.")

    # 실행 옵션이 있을 경우 실제 주문 실행
    if args.execute:
        # PyKis 초기화
        kis = initialize_kis(args.secret, args.virtual)

        # 매수 주문 실행
        results = execute_buy_orders(kis, df_buy)

        # 결과 저장
        results_df = pd.DataFrame(results)
        results_file = portfolio_file.parent / f"buy_results_{date_str}.csv"
        results_df.to_csv(results_file, index=False, encoding='utf-8-sig')
        print(f"\n주문 결과가 {results_file}에 저장되었습니다.")
    else:
        print("\n💡 매수 주문을 실행하려면 --execute 옵션을 사용하세요.")
        print(f"   예: python {__file__} --execute")


if __name__ == "__main__":
    main()