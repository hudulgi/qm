"""
포트폴리오 리밸런싱 스케줄러
portfolio_schedule.json에 지정된 날짜에만 buy_portfolio.py 실행

사용법:
1. OS 스케줄러로 매일 실행되도록 설정 (cron/Task Scheduler)
2. 스크립트가 오늘 날짜를 확인하고 거래일이면 buy_portfolio.py 실행
3. 거래일이 아니면 아무 작업도 하지 않음

예시:
python portfolio_scheduler.py --secret secret.json --execute --investment 10000000

Created on 2025-11-15
"""

import json
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path


def load_schedule(schedule_file='portfolio_schedule.json'):
    """
    스케줄 파일 로드

    Args:
        schedule_file: 스케줄 JSON 파일 경로

    Returns:
        dict: 스케줄 정보
    """
    if not os.path.exists(schedule_file):
        print(f"[오류] 스케줄 파일이 없습니다: {schedule_file}")
        sys.exit(1)

    try:
        with open(schedule_file, 'r', encoding='utf-8') as f:
            schedule = json.load(f)
        return schedule
    except Exception as e:
        print(f"[오류] 스케줄 파일 로드 실패: {e}")
        sys.exit(1)


def is_trading_day(schedule):
    """
    오늘이 거래일인지 확인

    Args:
        schedule: 스케줄 정보 딕셔너리

    Returns:
        bool: 거래일 여부
    """
    today = datetime.now().strftime("%Y-%m-%d")
    trading_dates = schedule.get('trading_dates', [])

    if today in trading_dates:
        print(f"\n✅ 오늘은 거래일입니다: {today}")
        return True
    else:
        print(f"\n⏭️  오늘은 거래일이 아닙니다: {today}")
        print(f"   다음 거래일 예정: {trading_dates}")
        return False


def execute_portfolio_script(args):
    """
    buy_portfolio.py 실행

    Args:
        args: 명령줄 인자 리스트 (portfolio_scheduler.py에 전달된 인자)

    Returns:
        int: 실행 결과 (0: 성공, 그 외: 실패)
    """
    # buy_portfolio.py 경로
    script_path = Path(__file__).parent / "buy_portfolio.py"

    if not script_path.exists():
        print(f"[오류] buy_portfolio.py를 찾을 수 없습니다: {script_path}")
        return 1

    # buy_portfolio.py 실행 명령 구성
    # portfolio_scheduler.py에 전달된 모든 인자를 그대로 전달
    cmd = [sys.executable, str(script_path)] + args

    print(f"\n{'='*80}")
    print(f"buy_portfolio.py 실행")
    print(f"{'='*80}")
    print(f"명령: {' '.join(cmd)}")
    print(f"{'='*80}\n")

    try:
        # buy_portfolio.py 실행
        result = subprocess.run(cmd, check=False)
        return result.returncode

    except Exception as e:
        print(f"\n[오류] buy_portfolio.py 실행 실패: {e}")
        return 1


def main():
    """메인 함수"""

    print(f"{'='*80}")
    print(f"포트폴리오 리밸런싱 스케줄러")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")

    # 스케줄 로드
    schedule = load_schedule()

    # 거래일 확인
    if not is_trading_day(schedule):
        print("\n종료합니다.")
        return

    # 거래일이면 buy_portfolio.py 실행
    # sys.argv[1:]은 portfolio_scheduler.py에 전달된 모든 인자 (--secret, --execute 등)
    args = sys.argv[1:]

    if not args:
        print("\n[경고] 전달할 인자가 없습니다.")
        print("사용 예시:")
        print("  python portfolio_scheduler.py --secret secret.json --execute --investment 10000000")
        print("  python portfolio_scheduler.py --secret secret.json --virtual secret_virtual.json --execute --investment 10000000")
        print("\n⚠️  buy_portfolio.py를 인자 없이 실행하면 오류가 발생할 수 있습니다.")
        print("계속하려면 Enter를 누르세요...")
        input()

    returncode = execute_portfolio_script(args)

    if returncode == 0:
        print(f"\n✅ buy_portfolio.py 실행 완료")
    else:
        print(f"\n⚠️  buy_portfolio.py 실행 실패 (exit code: {returncode})")


if __name__ == "__main__":
    main()