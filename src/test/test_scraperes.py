import pandas as pd
from ..boxoffice.logic.movie_events_scraper import (
    TheaterEventScraper,
    CGVScraper,
    LotteCinemaScraper,
    MegaboxScraper,
)
import logging

# 로깅 설정 (스크레이퍼와 동일하게)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_scraper(scraper: TheaterEventScraper):
    """
    개별 스크레이퍼의 이벤트 조회 및 재고 조회 기능을 테스트합니다.
    """
    logger = logging.getLogger("TestRunner")
    logger.info(f"--- {scraper.chain_name} 스크레이퍼 테스트 시작 ---")

    # 1. 이벤트 목록 조회
    events = scraper.get_events()
    if not events:
        logger.warning(f"{scraper.chain_name}: 조회된 이벤트가 없습니다.")
        logger.info(f"--- {scraper.chain_name} 스크레이퍼 테스트 종료 ---\n")
        return

    logger.info(f"{scraper.chain_name}: 총 {len(events)}개의 이벤트 조회 성공.")

    # 결과 확인을 위해 DataFrame으로 변환하여 출력
    events_df = pd.DataFrame(events)
    print("[이벤트 목록 (상위 5개)]")
    print(events_df.head())
    print("-" * 30)

    # 2. 첫 번째 이벤트의 재고 조회
    first_event = events[0]
    logger.info(f"'{first_event['goods_name']}' 굿즈의 재고 조회를 시작합니다.")

    stocks = scraper.get_goods_stock(first_event)
    if not stocks:
        logger.warning(f"{scraper.chain_name}: '{first_event['goods_name']}'의 재고 정보가 없습니다.")
        logger.info(f"--- {scraper.chain_name} 스크레이퍼 테스트 종료 ---\n")
        return

    logger.info(f"{scraper.chain_name}: 총 {len(stocks)}개 지점의 재고 정보 조회 성공.")

    # 재고 결과 확인
    stocks_df = pd.DataFrame(stocks)
    print("[재고 현황 (상위 10개)]")
    print(stocks_df.head(10))

    logger.info(f"--- {scraper.chain_name} 스크레이퍼 테스트 종료 ---\n")


def main():
    """모든 영화관 스크레이퍼를 테스트합니다."""
    scrapers = [
        CGVScraper(),
        LotteCinemaScraper(),
        MegaboxScraper(),
    ]

    for scraper in scrapers:
        test_scraper(scraper)

if __name__ == "__main__":
    main()