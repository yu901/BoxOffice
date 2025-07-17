import logging
import pandas as pd
from ..boxoffice.logic.sqlite_connector import SQLiteConnector
from ..boxoffice.logic.movie_events_scraper import (
    CGVScraper,
    LotteCinemaScraper,
    MegaboxScraper,
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def backfill_all_goods_events():
    """
    모든 영화관의 현재 진행중인 이벤트를 수집하여 DB에 저장합니다.
    이미 저장된 이벤트는 건너뛰고 신규 이벤트만 추가합니다.
    """
    logger = logging.getLogger("EventBackfill")
    db = SQLiteConnector()
    scrapers = [CGVScraper(), LotteCinemaScraper(), MegaboxScraper()]

    logger.info("모든 영화관의 이벤트 수집을 시작합니다...")
    all_events = []
    for scraper in scrapers:
        try:
            logger.info(f"{scraper.chain_name} 이벤트 수집 시작...")
            events = scraper.get_events()
            all_events.extend(events)
            logger.info(f"{scraper.chain_name} 이벤트 {len(events)}건 수집 완료.")
        except Exception as e:
            logger.error(f"{scraper.chain_name} 이벤트 수집 중 오류 발생: {e}", exc_info=True)
    
    if not all_events:
        logger.info("수집된 이벤트가 없습니다.")
        return

    new_events_df = pd.DataFrame(all_events).drop_duplicates(subset=['event_id'])
    db.insert_goods_event(new_events_df)
    logger.info(f"총 {len(new_events_df)}건의 이벤트를 DB에 저장(또는 업데이트)했습니다.")
    logger.info("이벤트 백필 작업 완료.")


if __name__ == "__main__":
    backfill_all_goods_events()