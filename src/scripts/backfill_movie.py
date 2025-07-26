import pandas as pd
from datetime import datetime
import logging
from tqdm import tqdm
from src.boxoffice.logic.kobisdata_extractor import KobisDataExtractor
from src.boxoffice.logic.sqlite_connector import SQLiteConnector

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def backfill_movies(start_year: int, end_year: int):
    """
    지정된 기간(연도) 동안의 영화 정보를 DB에 채워넣습니다.
    기존에 해당 기간의 데이터가 있다면 삭제 후 새로 삽입하여 중복을 방지합니다.
    :param start_year: 시작 연도 (YYYY)
    :param end_year: 종료 연도 (YYYY)
    """
    logger = logging.getLogger("BackfillMovies")
    logger.info(f"영화 정보 데이터 백필 시작: {start_year} ~ {end_year}")

    extractor = KobisDataExtractor()
    db = SQLiteConnector()

    # 1. 데이터베이스에서 기존 데이터 삭제 (Idempotency)
    conn = None
    try:
        logger.info(f"DB에서 기존 데이터 삭제 중 ({start_year} ~ {end_year})...")
        conn = db._get_connection()
        cursor = conn.cursor()
        # open_dt가 'YYYY-MM-DD' 형식의 텍스트로 저장되어 있다고 가정합니다.
        delete_query = f"DELETE FROM movie WHERE substr(open_dt, 1, 4) BETWEEN '{start_year}' AND '{end_year}'"
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
    all_movies_dfs = []
    year_range = range(start_year, end_year + 1)

    for year in tqdm(year_range, desc="연도별 영화 정보 수집 중"):
        yearly_df = extractor.get_MovieList(year)
        if not yearly_df.empty:
            all_movies_dfs.append(yearly_df)

    if not all_movies_dfs:
        logger.warning("API에서 추출된 데이터가 없습니다. 백필을 종료합니다.")
        return

    movies_df = pd.concat(all_movies_dfs, ignore_index=True)
    logger.info(f"총 {len(movies_df)}건의 영화 정보를 추출했습니다.")

    # 3. 데이터베이스에 삽입
    db.insert_movie(movies_df)
    logger.info(f"{len(movies_df)}건의 데이터를 성공적으로 DB에 삽입했습니다.")
    logger.info("백필 작업 완료.")

if __name__ == "__main__":
    # 예: 2024년부터 현재 연도까지의 데이터를 채워넣기
    start_year = 2025
    end_year = datetime.now().year
    backfill_movies(start_year, end_year)
