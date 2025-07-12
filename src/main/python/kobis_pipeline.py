from dagster import job, op, In, Out, get_dagster_logger, ScheduleDefinition
from datetime import datetime, timedelta
from src.main.python.kobisdata_extractor import KobisDataExtractor
from src.main.python.sqlite_connector import SQLiteConnector
import pandas as pd

@op(out={"boxoffice_df": Out(pd.DataFrame), "movie_df": Out(pd.DataFrame)})
def extract_kobis_data(context):
    logger = get_dagster_logger()
    extractor = KobisDataExtractor()

    # 어제 날짜
    target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"박스오피스 추출 날짜: {target_date}")

    # 1. boxoffice
    boxoffice_df = extractor.get_DailyBoxOffice(target_date, target_date)
    logger.info(f"박스오피스 수집 완료. {len(boxoffice_df)}건")

    # 2. movie list
    movie_df = extractor.get_MovieList(2025, 2025)
    logger.info(f"영화목록 수집 완료. {len(movie_df)}건")

    return boxoffice_df, movie_df

@op(ins={"boxoffice_df": In(pd.DataFrame), "movie_df": In(pd.DataFrame)})
def save_kobis_data(boxoffice_df, movie_df):
    logger = get_dagster_logger()
    db = SQLiteConnector()

    # 1. boxoffice 삽입
    db.insert_boxoffice(boxoffice_df)
    logger.info(f"{len(boxoffice_df)}건 boxoffice 삽입 완료")

    # 2. movie: 중복제거 후 삽입
    exist_movie_df = db.select_query("SELECT movieCd FROM movie")
    exist_codes = set(exist_movie_df["movieCd"].unique())
    new_movie_df = movie_df[~movie_df["movieCd"].isin(exist_codes)].copy()

    db.insert_movie(new_movie_df)
    logger.info(f"{len(new_movie_df)}건 신규 movie 삽입 완료")

@job
def kobis_daily_job():
    boxoffice_df, movie_df = extract_kobis_data()
    save_kobis_data(boxoffice_df, movie_df)

kobis_daily_schedule = ScheduleDefinition(
    job=kobis_daily_job,
    cron_schedule="0 8 * * *",
    name="daily_kobis_schedule",
    execution_timezone="Asia/Seoul"
)