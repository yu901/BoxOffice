import pandas as pd
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
from src.boxoffice.logic.kobisdata_extractor import KobisDataExtractor
from src.boxoffice.logic.sqlite_connector import SQLiteConnector

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def backfill_daily_boxoffice(start_date_str: str, end_date_str: str):
    """
    지정된 기간 동안의 일일 박스오피스 데이터를 DB에 채워넣습니다.
    기존에 해당 기간의 데이터가 있다면 삭제 후 새로 삽입하여 중복을 방지합니다.
    :param start_date_str: 시작일 (YYYYMMDD)
    :param end_date_str: 종료일 (YYYYMMDD)
    """
    logger = logging.getLogger("Backfill")
    logger.info(f"박스오피스 데이터 백필 시작: {start_date_str} ~ {end_date_str}")
    
    extractor = KobisDataExtractor()
    db = SQLiteConnector()
    
    start_dt = datetime.strptime(start_date_str, "%Y%m%d")
    end_dt = datetime.strptime(end_date_str, "%Y%m%d")

    # 1. 데이터베이스에서 기존 데이터 삭제 (Idempotency)
    conn = None
    try:
        logger.info(f"DB에서 기존 데이터 삭제 중 ({start_date_str} ~ {end_date_str})...")
        conn = db._get_connection()
        cursor = conn.cursor()
        start_dt_sql_format = start_dt.strftime('%Y-%m-%d')
        end_dt_sql_format = end_dt.strftime('%Y-%m-%d')
        delete_query = f"DELETE FROM boxoffice WHERE date(target_dt) >= date('{start_dt_sql_format}') AND date(target_dt) <= date('{end_dt_sql_format}')"
        cursor.execute(delete_query)
        conn.commit()
        logger.info(f"기존 데이터 {cursor.rowcount}건 삭제 완료.")
        cursor.close()
    except Exception as e:
        logger.error(f"기존 데이터 삭제 중 오류 발생: {e}")
        if conn:
            conn.rollback()
        return
    finally:
        if conn:
            conn.close()

    # 2. API에서 데이터 추출
    all_boxoffice_dfs = []
    date_range = [start_dt + timedelta(days=x) for x in range((end_dt - start_dt).days + 1)]
    
    for current_dt in tqdm(date_range, desc="일별 박스오피스 데이터 수집 중"):
        daily_df = extractor.get_DailyBoxOffice(current_dt)
        if not daily_df.empty:
            all_boxoffice_dfs.append(daily_df)
    
    if not all_boxoffice_dfs:
        logger.warning("API에서 추출된 데이터가 없습니다. 백필을 종료합니다.")
        return
    
    boxoffice_df = pd.concat(all_boxoffice_dfs, ignore_index=True)
    logger.info(f"총 {len(boxoffice_df)}건의 박스오피스 데이터를 추출했습니다.")
    
    # 3. 데이터베이스에 삽입
    db.insert_boxoffice(boxoffice_df)
    logger.info(f"{len(boxoffice_df)}건의 데이터를 성공적으로 DB에 삽입했습니다.")
    logger.info("백필 작업 완료.")

if __name__ == "__main__":
    # 예: 2024년 1월 1일부터 어제까지의 데이터를 채워넣기   
    start_date = "20250101"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    backfill_daily_boxoffice(start_date, end_date)