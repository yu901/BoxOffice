import sqlite3
import pandas as pd
from sqlalchemy import create_engine, types
from src.main.python.config import SQLiteConfig

class SQLiteConnector:
    def __init__(self):
        self.config = SQLiteConfig()
        self.engine = create_engine(f"sqlite:///{self.config.db_path}")
        self.conn = sqlite3.connect(self.config.db_path)
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS boxoffice (
                rnum INTEGER, rank INTEGER, rankInten INTEGER, rankOldAndNew TEXT,
                movieCd TEXT, movieNm TEXT, openDt DATE,
                salesAmt REAL, salesShare REAL, salesInten REAL, salesChange REAL, salesAcc REAL,
                audiCnt REAL, audiInten REAL, audiChange REAL, audiAcc REAL,
                scrnCnt REAL, showCnt REAL, targetDt DATE, elapsedDt INTEGER
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie (
                movieCd TEXT, movieNm TEXT, movieNmEn TEXT,
                prdtYear TEXT, openDt DATE, typeNm TEXT,
                prdtStatNm TEXT, nationAlt TEXT, genreAlt TEXT,
                repNationNm TEXT, repGenreNm TEXT,
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
                image_url TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goods_stock (
                scraped_at DATETIME,
                theater_name TEXT,
                event_id TEXT,
                status TEXT,
                quantity TEXT
            );
        """)
        self.conn.commit()
        cursor.close()

    def insert_boxoffice(self, df):
        df.to_sql("boxoffice", self.engine, if_exists='append', index=False)

    def insert_goods_event(self, df):
        """굿즈 이벤트 정보를 DB에 저장합니다."""
        df.to_sql("goods_event", self.engine, if_exists='append', index=False, dtype={
            'event_id': types.TEXT,
        })

    def insert_goods_stock(self, df):
        """굿즈 재고 정보를 DB에 저장합니다."""
        df.to_sql("goods_stock", self.engine, if_exists='append', index=False, dtype={
            'scraped_at': types.DateTime,
        })

    def insert_movie(self, df):
        df["directors"] = df["directors"].astype(str)
        df["companys"] = df["companys"].astype(str)
        df.to_sql("movie", self.engine, if_exists='append', index=False)

    def select_query(self, query):
        return pd.read_sql_query(query, self.conn)
