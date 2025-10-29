import urllib.request
import ssl
import zipfile
import os
import pandas as pd
import numpy as np
import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import logging
import time

from pykis import PyKis, KisStock, KisChart

# 로거 설정
script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
log_directory = "logs"
current_date_str = date.today().strftime("%Y%m%d")

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file_path = os.path.join(log_directory, f"{script_name}_{current_date_str}.log")

logger = logging.getLogger(script_name)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# 포트폴리오 선정 파라미터
TOP_MOMENTUM_COUNT = 100  # 모멘텀 상위 종목 수
BOTTOM_FIP_COUNT = 10     # FIP 하위 종목 수

# KIS 글로벌 변수
kis = None


def master_download(market, data_path):
    """
    KOSPI/KOSDAQ 마스터 파일을 다운로드하고 압축 해제

    Args:
        market: 'kospi' 또는 'kosdaq'
        data_path: 저장할 디렉토리 경로 (Path 객체)
    """
    zip_name = f'{market}_code.zip'

    ssl._create_default_https_context = ssl._create_unverified_context

    logger.info(f"{market.upper()} 마스터 파일 다운로드 중...")
    urllib.request.urlretrieve(
        f"https://new.real.download.dws.co.kr/common/master/{market}_code.mst.zip",
        data_path / zip_name
    )

    # 압축 해제
    with zipfile.ZipFile(data_path / zip_name) as zip_file:
        zip_file.extractall(path=data_path)

    # 압축 파일 삭제
    if (data_path / zip_name).exists():
        os.remove(data_path / zip_name)

    logger.info(f"{market.upper()} 마스터 파일 다운로드 완료")


def get_kospi_master_dataframe(data_path):
    """
    KOSPI 마스터 파일(.mst)을 파싱하여 DataFrame으로 변환

    Args:
        data_path: 마스터 파일이 있는 디렉토리 경로 (Path 객체)

    Returns:
        pd.DataFrame: KOSPI 종목 정보
    """
    file_name = data_path / "kospi_code.mst"
    tmp_fil1 = data_path / "kospi_code_part1.tmp"
    tmp_fil2 = data_path / "kospi_code_part2.tmp"

    wf1 = open(tmp_fil1, mode="w", encoding="utf-8")
    wf2 = open(tmp_fil2, mode="w", encoding="utf-8")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 228]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-228:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드', '표준코드', '한글명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='utf-8')

    field_specs = [2, 1, 4, 4, 4,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 9, 5, 5, 1,
                   1, 1, 2, 1, 1,
                   1, 2, 2, 2, 3,
                   1, 3, 12, 12, 8,
                   15, 21, 2, 7, 1,
                   1, 1, 1, 1, 9,
                   9, 9, 5, 9, 8,
                   9, 3, 1, 1, 1
                   ]

    part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                     '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                     'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                     'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                     'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                     'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                     'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                     '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                     '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                     '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                     '상장주수', '자본금', '결산월', '공모가', '우선주',
                     '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                     '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                     '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                     ]

    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

    df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

    # 임시 파일 삭제
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)

    logger.info(f"KOSPI 종목 {len(df)}개 로드 완료")

    return df


def get_kosdaq_master_dataframe(data_path):
    """
    KOSDAQ 마스터 파일(.mst)을 파싱하여 DataFrame으로 변환

    Args:
        data_path: 마스터 파일이 있는 디렉토리 경로 (Path 객체)

    Returns:
        pd.DataFrame: KOSDAQ 종목 정보
    """
    file_name = data_path / "kosdaq_code.mst"
    tmp_fil1 = data_path / "kosdaq_code_part1.tmp"
    tmp_fil2 = data_path / "kosdaq_code_part2.tmp"

    wf1 = open(tmp_fil1, mode="w", encoding="utf-8")
    wf2 = open(tmp_fil2, mode="w", encoding="utf-8")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 222]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-222:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드', '표준코드', '한글종목명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='utf-8')

    field_specs = [2, 1,
                   4, 4, 4, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 9,
                   5, 5, 1, 1, 1,
                   2, 1, 1, 1, 2,
                   2, 2, 3, 1, 3,
                   12, 12, 8, 15, 21,
                   2, 7, 1, 1, 1,
                   1, 9, 9, 9, 5,
                   9, 8, 9, 3, 1,
                   1, 1
                   ]

    part2_columns = ['증권그룹구분코드', '시가총액 규모 구분 코드 유가',
                     '지수업종 대분류 코드', '지수 업종 중분류 코드', '지수업종 소분류 코드', '벤처기업 여부 (Y/N)',
                     '저유동성종목 여부', 'KRX 종목 여부', 'ETP 상품구분코드', 'KRX100 종목 여부 (Y/N)',
                     'KRX 자동차 여부', 'KRX 반도체 여부', 'KRX 바이오 여부', 'KRX 은행 여부', '기업인수목적회사여부',
                     'KRX 에너지 화학 여부', 'KRX 철강 여부', '단기과열종목구분코드', 'KRX 미디어 통신 여부',
                     'KRX 건설 여부', '(코스닥)투자주의환기종목여부', 'KRX 증권 구분', 'KRX 선박 구분',
                     'KRX섹터지수 보험여부', 'KRX섹터지수 운송여부', 'KOSDAQ150지수여부 (Y,N)', '주식 기준가',
                     '정규 시장 매매 수량 단위', '시간외 시장 매매 수량 단위', '거래정지 여부', '정리매매 여부',
                     '관리 종목 여부', '시장 경고 구분 코드', '시장 경고위험 예고 여부', '불성실 공시 여부',
                     '우회 상장 여부', '락구분 코드', '액면가 변경 구분 코드', '증자 구분 코드', '증거금 비율',
                     '신용주문 가능 여부', '신용기간', '전일 거래량', '주식 액면가', '주식 상장 일자', '상장 주수(천)',
                     '자본금', '결산 월', '공모 가격', '우선주 구분 코드', '공매도과열종목여부', '이상급등종목여부',
                     'KRX300 종목 여부 (Y/N)', '매출액', '영업이익', '경상이익', '단기순이익', 'ROE(자기자본이익률)',
                     '기준년월', '전일기준 시가총액 (억)', '그룹사 코드', '회사신용한도초과여부', '담보대출가능여부', '대주가능여부'
                     ]

    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

    df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

    # 임시 파일 삭제
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)

    logger.info(f"KOSDAQ 종목 {len(df)}개 로드 완료")

    return df


def get_stock_code(filter_momentum=False):
    """
    KOSPI와 KOSDAQ 마스터 파일을 다운로드하고 종목 코드 리스트 반환

    Args:
        filter_momentum: True일 경우 모멘텀 계산용 종목만 필터링
                        - KOSPI: KOSPI200섹터업종에 값이 있는 종목
                        - KOSDAQ: KOSDAQ150지수여부가 'Y'인 종목

    Returns:
        list: KOSPI + KOSDAQ 종목 코드 리스트
    """
    base_dir = Path(__file__).parent
    data_path = base_dir / "data"
    data_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"데이터 디렉토리: {data_path}")

    # KOSPI 다운로드 및 처리
    logger.info("KOSPI 종목 코드 다운로드 중...")
    master_download('kospi', data_path)
    df_kospi = get_kospi_master_dataframe(data_path)
    df_kospi.to_csv(data_path / "kospi_code.csv", index=False)

    if filter_momentum:
        # KOSPI200섹터업종이 0이 아닌 종목만 필터링
        df_kospi_filtered = df_kospi[df_kospi['KOSPI200섹터업종'] != '0']
        code_kospi = df_kospi_filtered["단축코드"].tolist()
        logger.info(f"KOSPI 종목 필터링: 전체 {len(df_kospi)}개 중 KOSPI200섹터업종 {len(code_kospi)}개 선택")
    else:
        code_kospi = df_kospi["단축코드"].tolist()
        logger.info(f"KOSPI 종목 {len(code_kospi)}개 로드 완료.")

    # KOSDAQ 다운로드 및 처리
    logger.info("KOSDAQ 종목 코드 다운로드 중...")
    master_download('kosdaq', data_path)
    df_kosdaq = get_kosdaq_master_dataframe(data_path)
    df_kosdaq.to_csv(data_path / "kosdaq_code.csv", index=False)

    if filter_momentum:
        # KOSDAQ150지수여부가 'Y'인 종목만 필터링
        df_kosdaq_filtered = df_kosdaq[df_kosdaq['KOSDAQ150지수여부 (Y,N)'] == 'Y']
        code_kosdaq = df_kosdaq_filtered["단축코드"].tolist()
        logger.info(f"KOSDAQ 종목 필터링: 전체 {len(df_kosdaq)}개 중 KOSDAQ150 {len(code_kosdaq)}개 선택")
    else:
        code_kosdaq = df_kosdaq["단축코드"].tolist()
        logger.info(f"KOSDAQ 종목 {len(code_kosdaq)}개 로드 완료.")

    return code_kospi + code_kosdaq


def get_last_trading_date():
    """
    마지막 거래일을 찾아 반환

    여러 대형주를 체크하여 가장 최근 거래일을 찾음 (단일 종목 의존 위험 방지)

    Returns:
        str: 마지막 거래일 (YYYY-MM-DD)
    """
    # 여러 대형주로 확인 (삼성전자, SK하이닉스, NAVER, 카카오, 현대차)
    check_stocks = ["005930", "000660", "035420", "035720", "005380"]
    today = datetime.now()

    last_dates = []

    # 각 종목별로 최근 거래일 확인
    for code in check_stocks:
        for days_back in range(10):  # 최대 10일 전까지 확인
            check_date = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
            df = get_chart_data(code, check_date, check_date)

            if df is not None and len(df) > 0:
                last_date = df['Date'].max().strftime("%Y-%m-%d")
                last_dates.append(last_date)
                break

    if last_dates:
        # 가장 최근 날짜 선택
        last_trading_date = max(last_dates)
        logger.info(f"마지막 거래일: {last_trading_date} ({len(last_dates)}개 종목에서 확인)")
        return last_trading_date

    # 찾지 못한 경우 오늘 날짜 반환
    logger.warning("마지막 거래일을 찾지 못해 오늘 날짜 사용")
    return today.strftime("%Y-%m-%d")


def get_chart_data(code, start_date, end_date):
    """KIS API에서 차트 데이터를 직접 가져오기"""
    max_retries = 3
    retry_delay = 0.5

    for attempt in range(max_retries):
        try:
            _item: KisStock = kis.stock(code)
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            _chart: KisChart = _item.chart(start=start_dt, end=end_dt, adjust=True)

            data_list = []
            for record in _chart.bars:
                doc = {
                    "Date": record.time.strftime("%Y-%m-%d"),
                    "Open": float(record.open),
                    "High": float(record.high),
                    "Low": float(record.low),
                    "Close": float(record.close),
                    "Volume": float(record.volume)
                }
                data_list.append(doc)

            if len(data_list) == 0:
                return None

            df = pd.DataFrame(data_list)
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date').reset_index(drop=True)

            return df

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error for {code} (attempt {attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(retry_delay)
                continue
            else:
                logger.error(f"Final error for {code} after {max_retries} attempts: {str(e)}")
                return None

    return None


def calculate_momentum_and_fip_for_period(code, start_date, end_date):
    """특정 기간에 대한 수정 12개월 모멘텀과 FIP를 계산"""
    try:
        # 데이터 가져오기 (모멘텀 계산을 위해 13개월 전부터)
        data_start = (datetime.strptime(start_date, "%Y-%m-%d") - relativedelta(months=13)).strftime("%Y-%m-%d")
        df = get_chart_data(code, data_start, end_date)

        if df is None or len(df) < 250:
            return None

        df['Date'] = pd.to_datetime(df['Date'])

        # 필요한 기간 데이터만 추출
        df_period = df[(df['Date'] >= data_start) & (df['Date'] <= end_date)].copy()

        if len(df_period) < 250:  # 최소 1년 데이터 필요
            return None

        # 월별 마지막 거래일 데이터
        df_monthly = df_period.groupby(df_period['Date'].dt.to_period('M')).last()
        df_monthly.index = df_monthly.index.to_timestamp()
        df_monthly = df_monthly.sort_index()

        if len(df_monthly) < 13:
            return None

        # 수정 12개월 모멘텀 계산 (마지막 달 제외, 11개월)
        monthly_returns = df_monthly['Close'].pct_change().iloc[-12:-1] * 100  # 최근 12개월 중 11개월

        if len(monthly_returns.dropna()) < 10:
            return None

        momentum_12m_adj = 1.0
        for ret in monthly_returns.dropna():
            momentum_12m_adj *= (1 + ret / 100)
        momentum_12m_adj = (momentum_12m_adj - 1) * 100

        # FIP 계산을 위한 일간 데이터
        year_start = (datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(months=12)).strftime("%Y-%m-%d")
        df_daily = df_period[df_period['Date'] >= year_start].copy()

        if len(df_daily) < 200:
            return None

        df_daily['Daily_Return'] = df_daily['Close'].pct_change() * 100
        daily_returns = df_daily['Daily_Return'].dropna()

        positive_days = len(daily_returns[daily_returns > 0])
        negative_days = len(daily_returns[daily_returns < 0])
        total_days = len(daily_returns)

        if total_days == 0:
            return None

        positive_ratio = positive_days / total_days
        negative_ratio = negative_days / total_days

        momentum_sign = 1 if momentum_12m_adj > 0 else -1 if momentum_12m_adj < 0 else 0
        fip = momentum_sign * (negative_ratio - positive_ratio)

        # 마지막 월의 실제 마지막 거래일 찾기
        last_month_period = df_monthly.index[-1]
        last_month_data = df_period[df_period['Date'].dt.to_period('M') == last_month_period.to_period('M')]
        last_date = last_month_data['Date'].max()

        return {
            'code': code,
            'adjusted_momentum_12m': momentum_12m_adj,
            'fip': fip,
            'end_price': df_monthly.iloc[-1]['Close'],
            'end_price_date': last_date.strftime('%Y-%m-%d')
        }

    except Exception as e:
        logger.error(f"Error calculating momentum/FIP for {code}: {str(e)}")
        return None


def select_portfolio_stocks(stock_codes, end_date, top_momentum=TOP_MOMENTUM_COUNT, bottom_fip=BOTTOM_FIP_COUNT):
    """
    포트폴리오 종목 선정: 수정 모멘텀 상위 N개 중 FIP 하위 M개

    Args:
        stock_codes: 종목 코드 리스트
        end_date: 기준일 (YYYY-MM-DD)
        top_momentum: 모멘텀 상위 종목 수 (기본값: TOP_MOMENTUM_COUNT)
        bottom_fip: FIP 하위 종목 수 (기본값: BOTTOM_FIP_COUNT)

    Returns:
        list: 선정된 종목 정보 리스트
    """
    logger.info(f"\n{end_date} 기준 포트폴리오 종목 선정 중...")

    results = []
    processed = 0

    for code in stock_codes:
        processed += 1
        if processed % 50 == 0:
            logger.info(f"진행률: {processed}/{len(stock_codes)}")

        # 모멘텀 계산을 위해 기준일로부터 충분한 과거 데이터가 필요한 시작일 설정
        momentum_start = (datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(months=24)).strftime("%Y-%m-%d")
        result = calculate_momentum_and_fip_for_period(code, momentum_start, end_date)
        if result is not None:
            results.append(result)

    if len(results) < bottom_fip:
        logger.warning(f"데이터 부족 - {len(results)}개 종목만 분석됨")
        return []

    df_results = pd.DataFrame(results)

    # 수정 모멘텀 상위 종목 선정
    actual_top_momentum = min(top_momentum, len(df_results))
    df_top = df_results.nlargest(actual_top_momentum, 'adjusted_momentum_12m')

    # FIP 기준 오름차순 정렬하여 하위 N개 선정
    actual_bottom_fip = min(bottom_fip, len(df_top))
    df_portfolio = df_top.nsmallest(actual_bottom_fip, 'fip')

    # 종목명 추가
    base_dir = Path(__file__).parent
    data_path = base_dir / "data"

    # KOSPI와 KOSDAQ 마스터 파일 읽기
    df_kospi_master = pd.read_csv(data_path / "kospi_code.csv", encoding='utf-8')
    df_kosdaq_master = pd.read_csv(data_path / "kosdaq_code.csv", encoding='utf-8')

    # KOSPI는 '한글명', KOSDAQ는 '한글종목명' 컬럼 사용
    df_kospi_master = df_kospi_master[['단축코드', '한글명']].rename(columns={'한글명': '종목명'})
    df_kosdaq_master = df_kosdaq_master[['단축코드', '한글종목명']].rename(columns={'한글종목명': '종목명'})

    # 합치기
    df_master = pd.concat([df_kospi_master, df_kosdaq_master], ignore_index=True)

    # 종목명 매핑
    df_portfolio = df_portfolio.merge(df_master, left_on='code', right_on='단축코드', how='left')
    df_portfolio = df_portfolio.drop(columns=['단축코드'])

    # 컬럼 순서 재정렬
    cols = ['code', '종목명', 'adjusted_momentum_12m', 'fip', 'end_price', 'end_price_date']
    df_portfolio = df_portfolio[cols]

    logger.info(f"\n선정된 {actual_bottom_fip}개 종목:")
    for _, row in df_portfolio.iterrows():
        logger.info(f"  {row['code']} {row['종목명']}: 모멘텀 {row['adjusted_momentum_12m']:.2f}%, FIP {row['fip']:.4f}")

    return df_portfolio.to_dict('records')


def main():
    """메인 함수"""
    global kis

    # 명령줄 인수 파싱
    parser = argparse.ArgumentParser(description='모멘텀 및 FIP 기반 포트폴리오 종목 선정')
    parser.add_argument('--secret', required=True, help='KIS API secret 파일 경로 (필수)')
    args = parser.parse_args()

    # PyKis 초기화
    logger.info(f"KIS API 초기화: {args.secret}")
    kis = PyKis(args.secret, keep_token=True)

    # 모멘텀 계산용 필터링된 종목 코드
    print("=" * 50)
    print("모멘텀 계산용 종목 코드 다운로드")
    print("=" * 50)
    momentum_codes = get_stock_code(filter_momentum=True)
    print(f"\n모멘텀 계산 대상 종목 {len(momentum_codes)}개")
    print(f"처음 10개 종목 코드: {momentum_codes[:10]}")

    # 포트폴리오 선정
    print("\n" + "=" * 50)
    print("포트폴리오 선정")
    print("=" * 50)
    print(f"설정: 모멘텀 상위 {TOP_MOMENTUM_COUNT}개 중 FIP 하위 {BOTTOM_FIP_COUNT}개 선정")

    # 기준일 설정 (마지막 거래일 기준)
    end_date = get_last_trading_date()
    # end_date = "2024-12-31"  # 특정 날짜로 테스트하려면 이 줄을 사용

    selected_stocks = select_portfolio_stocks(
        stock_codes=momentum_codes,
        end_date=end_date,
        top_momentum=TOP_MOMENTUM_COUNT,
        bottom_fip=BOTTOM_FIP_COUNT
    )

    if selected_stocks:
        # 결과를 DataFrame으로 변환하여 저장
        df_portfolio = pd.DataFrame(selected_stocks)

        # portfolio 폴더 생성
        portfolio_dir = Path(__file__).parent / "portfolio"
        portfolio_dir.mkdir(parents=True, exist_ok=True)

        output_file = portfolio_dir / f"portfolio_{end_date}.csv"
        df_portfolio.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n포트폴리오가 {output_file}에 저장되었습니다.")
    else:
        print("\n포트폴리오 선정 실패")


if __name__ == "__main__":
    main()