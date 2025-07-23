from dagster import job, op, In, Out, get_dagster_logger, ScheduleDefinition
from datetime import datetime, timedelta
from ..logic.kobisdata_extractor import KobisDataExtractor
from ..logic.database_manager import get_database_connector
import pandas as pd
from typing import List, Dict

@op(out={"boxoffice_df": Out(pd.DataFrame), "movie_df": Out(pd.DataFrame)})
def extract_kobis_data(context):
    logger = get_dagster_logger()
    extractor = KobisDataExtractor()
    db = get_database_connector()

    # --- Box Office Backfill Logic ---
    yesterday = datetime.now() - timedelta(days=1)
    
    # DB에서 가장 최근 데이터 날짜 조회
    latest_date_df = db.select_query("SELECT MAX(targetDt) as max_date FROM boxoffice")
    
    start_date = None
    # DB에 데이터가 없거나 날짜가 없는 경우, 최근 7일치 수집
    if latest_date_df.empty or pd.isna(latest_date_df['max_date'].iloc[0]):
        logger.info("박스오피스 데이터가 없습니다. 최근 7일치 데이터를 수집합니다.")
        start_date = yesterday - timedelta(days=6)
    else:
        # 마지막 날짜의 다음날부터 수집 시작
        latest_date_in_db = pd.to_datetime(latest_date_df['max_date'].iloc[0]).date()
        start_date = (latest_date_in_db + timedelta(days=1))

    boxoffice_df = pd.DataFrame()
    all_boxoffice_dfs = []
    # 수집 시작일이 어제보다 이전일 경우에만 데이터 수집
    if start_date <= yesterday.date():
        logger.info(f"박스오피스 데이터 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {yesterday.strftime('%Y-%m-%d')}")
        current_date = start_date
        while current_date <= yesterday.date():
            daily_df = extractor.get_DailyBoxOffice(datetime.combine(current_date, datetime.min.time()))
            if not daily_df.empty:
                all_boxoffice_dfs.append(daily_df)
            current_date += timedelta(days=1)
        
        if all_boxoffice_dfs:
            boxoffice_df = pd.concat(all_boxoffice_dfs, ignore_index=True)
        else:
            boxoffice_df = pd.DataFrame(columns=['rnum', 'rank', 'rankInten', 'rankOldAndNew', 'movieCd', 'movieNm', 'openDt', 'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt'])
        logger.info(f"박스오피스 수집 완료. {len(boxoffice_df)}건")
    else:
        logger.info("박스오피스 데이터가 최신 상태입니다. 수집을 건너뜁니다.")
        boxoffice_df = pd.DataFrame(columns=['rnum', 'rank', 'rankInten', 'rankOldAndNew', 'movieCd', 'movieNm', 'openDt', 'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt'])

    # 2. movie list
    current_year = datetime.now().year
    movie_df = extractor.get_MovieList(current_year)
    logger.info(f"영화목록 수집 완료. {len(movie_df)}건")

    if movie_df.empty:
        movie_df = pd.DataFrame(columns=['movieCd', 'movieNm', 'movieNmEn', 'prdtYear', 'openDt', 'typeNm', 'prdtStatNm', 'nationAlt', 'genreAlt', 'repNationNm', 'repGenreNm', 'directors', 'companys'])

    return boxoffice_df, movie_df

@op(
    ins={"boxoffice_df": In(pd.DataFrame), "movie_df": In(pd.DataFrame)}
)
def save_kobis_data(boxoffice_df, movie_df):
    logger = get_dagster_logger()
    db = get_database_connector()

    # 1. boxoffice 삽입
    if not boxoffice_df.empty:
        db.insert_boxoffice(boxoffice_df)
        logger.info(f"{len(boxoffice_df)}건 boxoffice 삽입 완료")
    else:
        logger.info("삽입할 신규 박스오피스 데이터가 없습니다.")

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