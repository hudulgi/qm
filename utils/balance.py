from dataclasses import dataclass


@dataclass
class StockHolding:
    symbol: str      # pdno (종목코드)
    name: str        # prdt_name (종목명)
    quantity: int    # hldg_qty (보유수량) - 합산
    orderable: int   # ord_psbl_qty (주문가능수량) - 합산


def fetch_domestic_balance(kis, logger):
    """KIS REST API를 직접 호출하여 국내주식 잔고를 조회한다.

    pykis의 account.balance()가 INQR_DVSN=02(종목별)를 사용하지만,
    KIS에서 해당 입력을 제한했으므로 INQR_DVSN=01(대출일별)로 직접 조회.
    INQR_DVSN=01은 같은 종목이 매입일자별로 여러 행 반환 -> 종목코드 기준 집계.

    Returns:
        (account_number: str, stocks: list[StockHolding], summary: dict)
        summary keys: total (총평가금액), available_cash (예수금총금액)
    """
    acct = kis.primary
    cano = acct.number        # 8자리 계좌번호
    acnt_prdt_cd = acct.code  # 2자리 상품코드

    tr_id = "VTTC8434R" if kis.virtual else "TTTC8434R"
    account_number = f"{cano}-{acnt_prdt_cd}"

    # 종목코드별 집계 딕셔너리: {pdno: {name, quantity, orderable}}
    agg = {}
    summary = {}

    ctx_area_fk100 = ""
    ctx_area_nk100 = ""

    while True:
        resp = kis.fetch(
            "/uapi/domestic-stock/v1/trading/inquire-balance",
            api=tr_id,
            params={
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "01",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": ctx_area_fk100,
                "CTX_AREA_NK100": ctx_area_nk100,
            },
        )

        data = resp.__data__

        for item in data.get("output1", []):
            pdno = item.get("pdno", "").strip()
            if not pdno:
                continue
            qty = int(item.get("hldg_qty", 0))
            ord_qty = int(item.get("ord_psbl_qty", 0))
            name = item.get("prdt_name", "").strip()

            if pdno in agg:
                agg[pdno]["quantity"] += qty
                agg[pdno]["orderable"] += ord_qty
            else:
                agg[pdno] = {
                    "name": name,
                    "quantity": qty,
                    "orderable": ord_qty,
                }

        # output2에서 계좌 요약 정보 추출
        output2 = data.get("output2", [])
        if output2:
            o2 = output2[0] if isinstance(output2, list) else output2
            summary = {
                "total": int(o2.get("tot_evlu_amt", 0)),
                "available_cash": int(o2.get("dnca_tot_amt", 0)),
            }

        # 연속조회 키 갱신
        ctx_area_fk100 = data.get("ctx_area_fk100", "").strip()
        ctx_area_nk100 = data.get("ctx_area_nk100", "").strip()

        if not ctx_area_nk100:
            break

    stocks = [
        StockHolding(
            symbol=pdno,
            name=info["name"],
            quantity=info["quantity"],
            orderable=info["orderable"],
        )
        for pdno, info in agg.items()
        if info["quantity"] > 0
    ]

    logger.info(f"잔고 조회 완료: 계좌 {account_number}, {len(stocks)}종목")
    for s in stocks:
        logger.info(f"  {s.symbol} {s.name} 보유{s.quantity} 주문가능{s.orderable}")

    return account_number, stocks, summary