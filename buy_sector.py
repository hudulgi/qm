"""
섹터 ETF 로테이션 전략 실행
6개월 토탈리턴 상위 TOP_N개 섹터 ETF에 균등 투자하는 모멘텀 전략 (월 1회)

buy_gem.py의 인증/시세/토탈리턴/잔고 함수를 재사용한다 (룩백은 buy_gem.MOMENTUM_MONTHS 공유).
buy_gem과 달리 이 스크립트는 SECTOR_ETFS에 정의된 종목만 사고팔며,
계좌의 다른 보유 종목(GEM 등 타 전략 보유분)은 건드리지 않는다.

백테스트 근거 (2013-07~2026-07, 월간, 수정주가):
  top3/6M CAGR 16.6% Sharpe 0.66 MDD -44.9% vs KODEX200 12.4%/0.57/-36.6%

사용:
  python buy_sector.py --secret secret.json              # 분석만 (주문 없음)
  python buy_sector.py --secret secret.json --execute    # 실제 리밸런싱
  python buy_sector.py --secret secret.json --investment 10000000 --execute
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import buy_gem
from buy_gem import (
    MAX_RETRIES, RETRY_DELAY, ORDER_DELAY, REBALANCE_WAIT_TIME, BUFFER_RATIO,
    initialize_kis, calculate_total_return, get_current_holdings,
    get_current_price, get_total_balance,
)
from utils.secret_loader import resolve_secret

# 전략 설정
SECTOR_ETFS = {
    "091160": "KODEX 반도체",
    "139260": "TIGER 200 IT",
    "091180": "KODEX 자동차",
    "139270": "TIGER 200 금융",
    "102970": "KODEX 증권",
    "140700": "KODEX 보험",
    "117460": "KODEX 에너지화학",
    "117680": "KODEX 철강",
    "117700": "KODEX 건설",
    "139230": "TIGER 200 중공업",
    "139290": "TIGER 200 경기소비재",
    "140710": "KODEX 운송",
    "143860": "TIGER 헬스케어",
    "157490": "TIGER 소프트웨어",
}
TOP_N = 3  # 보유 섹터 수 (백테스트: 2~3이 최적, 4 이상은 지수 수준으로 희석)
EXECUTION_LOG_FILE = "sector_execution_log.json"
LOG_DIR = "logs"

logger = None


def setup_logger():
    global logger
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    logger = logging.getLogger("sector")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_file = os.path.join(LOG_DIR, f"sector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    for h in (logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()):
        h.setFormatter(fmt)
        logger.addHandler(h)
    # buy_gem에서 임포트한 함수들이 내부적으로 buy_gem.logger를 참조하므로 연결
    buy_gem.logger = logger
    return logger


def check_monthly_execution(force=False):
    """이번 달 실행 여부 확인 (월 1회 제한)"""
    if force:
        return True
    if not os.path.exists(EXECUTION_LOG_FILE):
        return True
    try:
        with open(EXECUTION_LOG_FILE, 'r', encoding='utf-8') as f:
            log = json.load(f)
    except (json.JSONDecodeError, OSError):
        return True
    this_month = datetime.now().strftime("%Y-%m")
    for entry in log.get("executions", []):
        if entry.get("month") == this_month and entry.get("success"):
            logger.warning(f"이번 달({this_month})은 이미 실행되었습니다. (--force로 무시 가능)")
            return False
    return True


def record_execution(picks, success):
    log = {"executions": []}
    if os.path.exists(EXECUTION_LOG_FILE):
        try:
            with open(EXECUTION_LOG_FILE, 'r', encoding='utf-8') as f:
                log = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    log.setdefault("executions", []).append({
        "month": datetime.now().strftime("%Y-%m"),
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "picks": picks,
        "success": success,
    })
    with open(EXECUTION_LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def order_with_retry(kis, code, name, side, qty, price=None):
    """매수/매도 주문 (재시도 포함). side: 'buy'|'sell'. price=None이면 시장가 매도/현재가 매수 불가."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                logger.warning(f"[재시도 {attempt}/{MAX_RETRIES}]")
                time.sleep(RETRY_DELAY * (attempt - 1))
            stock = kis.stock(code)
            if side == 'sell':
                order = stock.sell(price=price, qty=qty, condition=None, execution=None)
            else:
                order = stock.buy(price=price, qty=qty, condition=None, execution=None)
            num = order.number if hasattr(order, 'number') else 'N/A'
            logger.info(f"[{'매도' if side == 'sell' else '매수'} 성공] {code} ({name}) {qty}주, 주문번호: {num}")
            return True
        except Exception as e:
            msg = str(e).lower()
            no_retry = ['잔고', '부족', '수량', '불가', '영업일', '장마감', '장종료', '장시작전', '매매거래정지']
            if any(k in msg for k in no_retry) or attempt == MAX_RETRIES:
                logger.error(f"[{'매도' if side == 'sell' else '매수'} 실패] {code} ({name}): {e}")
                return False
    return False


def main():
    parser = argparse.ArgumentParser(description='섹터 ETF 로테이션 전략 실행')
    parser.add_argument('--secret', required=True, help='실전 계좌 secret 파일 경로')
    parser.add_argument('--virtual', help='모의투자 계좌 secret 파일 경로')
    parser.add_argument('--investment', type=int, help='총 투자액 (기본: 현재 총평가금액)')
    parser.add_argument('--execute', action='store_true', help='실제 주문 실행 (없으면 분석만)')
    parser.add_argument('--force', action='store_true', help='월 1회 제한 무시')
    parser.add_argument('--top', type=int, default=TOP_N, help=f'보유 섹터 수 (기본 {TOP_N})')
    args = parser.parse_args()

    setup_logger()
    logger.info("=" * 80)
    logger.info(f"섹터 ETF 로테이션 전략 — {buy_gem.MOMENTUM_MONTHS}개월 토탈리턴 상위 {args.top}개 균등 투자")
    logger.info("=" * 80)

    if not check_monthly_execution(args.force):
        sys.exit(0)

    args.secret = resolve_secret(args.secret)
    kis = initialize_kis(args.secret, args.virtual)
    is_virtual = args.virtual is not None

    # 1. 전체 섹터 토탈리턴 분석
    results = []
    for code, name in SECTOR_ETFS.items():
        r = calculate_total_return(kis, code, name, logger)
        if r:
            results.append(r)
            logger.info(f"  {code} {name}: {r['total_return']:+.2f}%")
        else:
            logger.warning(f"  {code} {name}: 분석 실패")
        time.sleep(0.5)

    if len(results) < args.top:
        logger.error(f"분석 성공 종목이 {len(results)}개뿐입니다. 중단합니다.")
        record_execution([], False)
        sys.exit(1)

    results.sort(key=lambda x: x['total_return'], reverse=True)
    picks = results[:args.top]

    logger.info("\n" + "=" * 80)
    logger.info(f"선정 섹터 (상위 {args.top}개):")
    for i, r in enumerate(results, 1):
        marker = "🥇" if i <= args.top else "  "
        logger.info(f"{marker} {i:2d}. {r['stock_code']} {r['stock_name']:20s} {r['total_return']:+8.2f}%")

    # 2. 리밸런싱 계획
    total_investment = args.investment or get_total_balance(kis)
    if not total_investment:
        logger.error("총투자액 확인 실패")
        sys.exit(1)
    per_amount = int(total_investment * BUFFER_RATIO / args.top)
    pick_codes = [r['stock_code'] for r in picks]

    holdings = get_current_holdings(kis)
    managed = {c: h for c, h in holdings.items() if c in SECTOR_ETFS}  # 이 전략이 관리하는 보유분만

    plan = {'sell_all': [], 'adjust': []}
    for code, h in managed.items():
        if code not in pick_codes:
            plan['sell_all'].append((code, h['name'], h['qty']))
    for r in picks:
        code = r['stock_code']
        price = get_current_price(kis, code)
        if price is None:
            logger.error(f"현재가 조회 실패: {code} — 해당 종목 건너뜀")
            continue
        target_qty = per_amount // price
        held = managed.get(code, {}).get('qty', 0)
        plan['adjust'].append((code, r['stock_name'], price, held, target_qty))
        time.sleep(0.3)

    logger.info("\n" + "=" * 80)
    logger.info(f"리밸런싱 계획 (총투자액 {total_investment:,}원, 섹터당 {per_amount:,}원)")
    logger.info("=" * 80)
    for code, name, qty in plan['sell_all']:
        logger.info(f"  [전량매도] {code} {name}: {qty}주")
    for code, name, price, held, target in plan['adjust']:
        diff = target - held
        action = "매수" if diff > 0 else "매도" if diff < 0 else "유지"
        logger.info(f"  [{action}] {code} {name}: 보유 {held} → 목표 {target}주 (현재가 {price:,}원)")

    if not args.execute:
        logger.info("\n💡 실제 주문을 실행하려면 --execute 옵션을 사용하세요.")
        return

    # 3. 주문 실행: 매도(제외 섹터 전량 + 초과분) → 대기 → 매수
    success = True
    sold_any = False
    for code, name, qty in plan['sell_all']:
        success &= order_with_retry(kis, code, name, 'sell', qty, price=None)  # 시장가
        sold_any = True
        time.sleep(ORDER_DELAY)
    for code, name, price, held, target in plan['adjust']:
        if target < held:
            success &= order_with_retry(kis, code, name, 'sell', held - target, price=None)
            sold_any = True
            time.sleep(ORDER_DELAY)

    if sold_any:
        logger.info(f"[대기] 매도 체결 대기 {REBALANCE_WAIT_TIME}초...")
        time.sleep(REBALANCE_WAIT_TIME)

    for code, name, price, held, target in plan['adjust']:
        if target > held:
            buy_price = get_current_price(kis, code) or price
            success &= order_with_retry(kis, code, name, 'buy', target - held, price=buy_price)
            time.sleep(ORDER_DELAY)

    record_execution(pick_codes, success)
    logger.info("\n" + "=" * 80)
    logger.info("✅ 리밸런싱 완료" if success else "⚠️ 일부 주문 실패 — 로그를 확인하세요")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
