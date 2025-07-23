from dagster import job, op, In, Out, get_dagster_logger, ScheduleDefinition
from datetime import datetime
from ..logic.database_manager import get_database_connector
import pandas as pd
from typing import List, Dict

from ..logic.movie_events_scraper import (
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

@op(out=Out(List[Dict]))
def get_events_from_db() -> List[Dict]:
    """DB에 저장된 이벤트 목록 중 종료되지 않은 이벤트만 가져옵니다."""
    logger = get_dagster_logger()
    db = get_database_connector()
    events_df = db.select_query("SELECT * FROM goods_event")

    if events_df.empty:
        logger.info("DB에 저장된 이벤트가 없습니다.")
        return []

    # 종료일이 지난 이벤트는 필터링합니다.
    # errors='coerce'는 잘못된 날짜 형식을 NaT (Not a Time)으로 변환하여 오류를 방지합니다.
    events_df['end_date_dt'] = pd.to_datetime(events_df['end_date'], errors='coerce')
    active_events_df = events_df[events_df['end_date_dt'] >= pd.Timestamp.now().normalize()].copy()
    
    logger.info(f"{len(active_events_df)}개의 이벤트를 대상으로 재고를 조회합니다.")
    
    return active_events_df.to_dict('records')

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
            logger.info(f"'{event.get('goods_name', 'N/A')}' 재고 조회 시작...")
            stocks = scraper.get_goods_stock(event)
            logger.info(f"'{event.get('goods_name', 'N/A')}' 재고 {len(stocks)}건 조회 완료.")
            
            for stock in stocks:
                enriched_stock = {
                    "event_id": event["event_id"],
                    "theater_name": stock["theater_name"],
                    "status": stock["status"],
                    "quantity": stock["quantity"],
                    "total_quantity": stock.get("total_quantity")
                }
                all_stocks_enriched.append(enriched_stock)
        except Exception as e:
            logger.error(f"'{event.get('goods_name', 'N/A')}' 재고 조회 중 오류 발생: {e}", exc_info=True)

    return all_stocks_enriched

@op(ins={"events": In(List[Dict])})
def save_events_to_db(events: List[Dict]):
    """수집된 이벤트 정보를 데이터베이스에 저장하거나 업데이트합니다."""
    logger = get_dagster_logger()
    if not events:
        logger.info("저장할 이벤트 정보가 없습니다.")
        return

    db = get_database_connector()
    db.insert_goods_event(events)
    logger.info(f"총 {len(events)}건의 이벤트 정보를 DB에 저장/업데이트했습니다.")

@op(ins={"stocks": In(List[Dict])}, out=Out(pd.DataFrame))
def save_stocks_to_db(stocks: List[Dict]):
    """재고 정보를 데이터베이스에 저장합니다."""
    logger = get_dagster_logger()
    if not stocks:
        logger.info("저장할 재고 정보가 없습니다.")
        return pd.DataFrame()

    db = get_database_connector()
    stocks_df = pd.DataFrame(stocks)
    stocks_df["scraped_at"] = datetime.now()
    
    db.insert_goods_stock(stocks_df)
    logger.info(f"총 {len(stocks_df)}건의 재고 정보를 DB에 저장했습니다.")
    return stocks_df

@job
def goods_events_job():
    """매일 아침 영화관 굿즈 이벤트를 수집하여 저장하는 작업"""
    events = get_all_events()
    save_events_to_db(events)

@job
def goods_stock_check_job():
    """영화관 굿즈 재고를 주기적으로 확인하고 저장하는 작업"""
    events = get_events_from_db()
    stocks = get_all_stocks(events)
    stocks_df_result = save_stocks_to_db(stocks)

goods_events_schedule = ScheduleDefinition(
    job=goods_events_job,
    cron_schedule="0 8 * * *",  # 매일 아침 8시에 실행
    name="daily_goods_events_check",
    execution_timezone="Asia/Seoul"
)

goods_stock_schedule = ScheduleDefinition(
    job=goods_stock_check_job,
    cron_schedule="*/10 * * * *",  # 10분마다 실행
    name="periodic_goods_stock_check",
    execution_timezone="Asia/Seoul"
)