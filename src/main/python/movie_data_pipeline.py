from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.main.python.sqlite_connector import SQLiteConnector
from src.main.python.kobisdata_extractor import KobisDataExtractor
import pandas as pd

# (1) DB 초기화 함수
def init_sqlite_db():
    SQLiteConnector()  # 생성 시 테이블 자동 생성

# (2) 영화 목록 저장
def insert_movie_data():
    extractor = KobisDataExtractor()
    db = SQLiteConnector()

    movie_df = extractor.get_MovieList(start_year=2024, end_year=2024)
    db.insert_movie(movie_df)

# (3) 박스오피스 저장
def insert_boxoffice_data():
    extractor = KobisDataExtractor()
    db = SQLiteConnector()

    # 예시: 2024년 1월 1일~5일 박스오피스
    boxoffice_df = extractor.get_DailyBoxOffice("20240101", "20240105")
    db.insert_boxoffice(boxoffice_df)

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='kobis_movie_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행 (테스트 시 수동 실행 권장)
    catchup=False,
    tags=['kobis', 'sqlite', 'movie'],
) as dag:

    t1_init_db = PythonOperator(
        task_id='init_sqlite_db',
        python_callable=init_sqlite_db,
    )

    t2_insert_movie = PythonOperator(
        task_id='insert_movie_data',
        python_callable=insert_movie_data,
    )

    t3_insert_boxoffice = PythonOperator(
        task_id='insert_boxoffice_data',
        python_callable=insert_boxoffice_data,
    )

    # Task 순서 설정
    t1_init_db >> [t2_insert_movie, t3_insert_boxoffice]
