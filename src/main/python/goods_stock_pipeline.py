from dagster import job, op, In, Out, get_dagster_logger, ScheduleDefinition
from datetime import datetime
from src.main.python.sqlite_connector import SQLiteConnector
import pandas as pd
from typing import List, Dict

from src.main.python.movie_events_scraper import (
    CGVScraper,
    LotteCinemaScraper,
    MegaboxScraper,
    UnifiedEvent,
)

SCRAPERS = [CGVScraper(), LotteCinemaScraper(), MegaboxScraper()]

@op(out=Out(List[Dict]))
def get_all_events() -> List[UnifiedEvent]:
    """모든 영화관의 현재 진행중인 굿즈 이벤트를 수집합니다."""
    logger = get_dagster_logger()
    all_events = []
    for scraper in SCRAPERS:
        try:
            logger.info(f"{scraper.chain_name} 이벤트 수집 시작...")
            events = scraper.get_events()
            all_events.extend(events)
            logger.info(f"{scraper.chain_name} 이벤트 {len(events)}건 수집 완료.")
        except Exception as e:
            logger.error(f"{scraper.chain_name} 이벤트 수집 중 오류 발생: {e}", exc_info=True)
    return all_events

@op(ins={"events": In(List[Dict])}, out=Out(List[Dict]))
def get_all_stocks(events: List[Dict]) -> List[Dict]:
    """수집된 모든 이벤트에 대해 재고 정보를 수집합니다."""
    logger = get_dagster_logger()
    if not events:
        logger.info("수집된 이벤트가 없어 재고 조회를 건너뜁니다.")
        return []

    all_stocks_enriched = []
    scraper_map = {scraper.chain_name: scraper for scraper in SCRAPERS}

    for event in events:
        scraper = scraper_map.get(event["theater_chain"])
        if not scraper:
            logger.warning(f"'{event['theater_chain']}'에 해당하는 스크레이퍼를 찾을 수 없습니다.")
            continue
        
        try:
            logger.info(f"'{event['goods_name']}' 재고 조회 시작...")
            stocks = scraper.get_goods_stock(event)
            logger.info(f"'{event['goods_name']}' 재고 {len(stocks)}건 조회 완료.")
            
            for stock in stocks:
                enriched_stock = {
                    "theater_chain": event["theater_chain"],
                    "event_title": event["event_title"],
                    "movie_title": event["movie_title"],
                    "goods_name": event["goods_name"],
                    "theater_name": stock["theater_name"],
                    "status": stock["status"],
                    "quantity": stock["quantity"],
                }
                all_stocks_enriched.append(enriched_stock)
        except Exception as e:
            logger.error(f"'{event['goods_name']}' 재고 조회 중 오류 발생: {e}", exc_info=True)

    return all_stocks_enriched

@op(ins={"stocks": In(List[Dict])})
def save_stocks_to_db(stocks: List[Dict]):
    """재고 정보를 데이터베이스에 저장합니다."""
    logger = get_dagster_logger()
    if not stocks:
        logger.info("저장할 재고 정보가 없습니다.")
        return

    db = SQLiteConnector()
    stocks_df = pd.DataFrame(stocks)
    stocks_df["scraped_at"] = datetime.now()
    
    db.insert_goods_stock(stocks_df)
    logger.info(f"총 {len(stocks_df)}건의 재고 정보를 DB에 저장했습니다.")

@job
def goods_stock_check_job():
    """영화관 굿즈 재고를 주기적으로 확인하고 저장하는 작업"""
    events = get_all_events()
    stocks = get_all_stocks(events)
    save_stocks_to_db(stocks)

goods_stock_schedule = ScheduleDefinition(
    job=goods_stock_check_job,
    cron_schedule="*/1 * * * *",  # 10분마다 실행
    name="periodic_goods_stock_check",
    execution_timezone="Asia/Seoul"
)