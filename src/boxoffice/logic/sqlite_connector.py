import sqlite3
import pandas as pd
from sqlalchemy import create_engine, types
import re
from typing import List, Dict
from .config import SQLiteConfig
from .base_connector import BaseDatabaseConnector

class SQLiteConnector(BaseDatabaseConnector):
    def __init__(self):
        self.config = SQLiteConfig()
        self.db_path = self.config.db_path
        self.engine = create_engine(f"sqlite:///{self.db_path}")

        self.create_tables()

    def _get_connection(self):
        """새로운 sqlite3 커넥션을 생성하여 반환합니다."""
        return sqlite3.connect(self.db_path)

    def create_tables(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS boxoffice (
                rnum INTEGER, rank INTEGER, rank_inten INTEGER, rank_old_and_new TEXT,
                movie_cd TEXT, movie_nm TEXT, open_dt DATE,
                sales_amt REAL, sales_share REAL, sales_inten REAL, sales_change REAL, sales_acc REAL,
                audi_cnt REAL, audi_inten REAL, audi_change REAL, audi_acc REAL,
                scrn_cnt REAL, show_cnt REAL, target_dt DATE, elapsed_dt INTEGER
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie (
                movie_cd TEXT, movie_nm TEXT, movie_nm_en TEXT,
                prdt_year TEXT, open_dt DATE, type_nm TEXT,
                prdt_stat_nm TEXT, nation_alt TEXT, genre_alt TEXT,
                rep_nation_nm TEXT, rep_genre_nm TEXT,
                directors TEXT, companys TEXT
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS goods_event (
                event_id TEXT PRIMARY KEY,
                theater_chain TEXT,
                event_title TEXT,
                movie_title TEXT,
                goods_name TEXT,
                goods_id TEXT,
                start_date TEXT,
                end_date TEXT,
                event_url TEXT,
                image_url TEXT,
                spmtl_no TEXT
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS goods_stock (
                scraped_at DATETIME,
                theater_name TEXT,
                event_id TEXT,
                status TEXT,
                quantity TEXT,
                total_quantity INTEGER
            );
            """)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def insert_boxoffice(self, df: pd.DataFrame):
        df.to_sql("boxoffice", self.engine, if_exists='append', index=False)

    def insert_goods_event(self, events: List[Dict]):
        """굿즈 이벤트 정보를 DB에 저장합니다. ON CONFLICT를 사용하여 업데이트합니다."""
        if not events:
            return

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            upsert_query = """
                INSERT INTO goods_event (
                    event_id, theater_chain, event_title, movie_title, goods_name,
                    goods_id, start_date, end_date, event_url, image_url, spmtl_no
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    theater_chain = excluded.theater_chain,
                    event_title = excluded.event_title,
                    movie_title = excluded.movie_title,
                    goods_name = excluded.goods_name,
                    goods_id = excluded.goods_id,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    event_url = excluded.event_url,
                    image_url = excluded.image_url,
                    spmtl_no = excluded.spmtl_no
                WHERE event_id = excluded.event_id;
            """
            # 딕셔너리 리스트를 튜플 리스트로 변환
            data_to_insert = [
                (event.get("event_id"), event.get("theater_chain"), event.get("event_title"),
                 event.get("movie_title"), event.get("goods_name"), event.get("goods_id"),
                 event.get("start_date"), event.get("end_date"), event.get("event_url"),
                 event.get("image_url"), event.get("spmtl_no"))
                for event in events
            ]
            cursor.executemany(upsert_query, data_to_insert)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def insert_goods_stock(self, df: pd.DataFrame):
        """굿즈 재고 정보를 DB에 저장합니다."""
        df.to_sql("goods_stock", self.engine, if_exists='append', index=False, dtype={
            'scraped_at': types.DateTime,
        })

    def insert_movie(self, df: pd.DataFrame):
        df.columns = [self._get_db_column_name(col) for col in df.columns]
        df.to_sql("movie", self.engine, if_exists='append', index=False)

    def select_query(self, query: str) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            return pd.read_sql_query(query, conn)
        finally:
            conn.close()

    def _get_db_column_name(self, logical_name: str) -> str:
        return logical_name
