import sqlite3
import pandas as pd
from sqlalchemy import create_engine, types
from src.boxoffice.logic.config import SQLiteConfig

class SQLiteConnector:
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
                image_url TEXT,
                spmtl_no TEXT,
                total_given_quantity INTEGER
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS goods_stock (
                scraped_at DATETIME,
                theater_name TEXT,
                event_id TEXT,
                status TEXT,
                quantity TEXT,
                total_given_quantity INTEGER
            );
            """)
            # Add spmtl_no column to goods_event if it doesn't exist
            try:
                cursor.execute("ALTER TABLE goods_event ADD COLUMN spmtl_no TEXT;")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise
            # Add total_given_quantity column to goods_event if it doesn't exist
            try:
                cursor.execute("ALTER TABLE goods_event ADD COLUMN total_given_quantity INTEGER;")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise
            # Add total_given_quantity column to goods_stock if it doesn't exist
            try:
                cursor.execute("ALTER TABLE goods_stock ADD COLUMN total_given_quantity INTEGER;")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise
            conn.commit()
        finally:
            cursor.close()
            conn.close()

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
            'total_given_quantity': types.INTEGER
        })

    def insert_movie(self, df):
        df["directors"] = df["directors"].astype(str)
        df["companys"] = df["companys"].astype(str)
        df.to_sql("movie", self.engine, if_exists='append', index=False)

    def select_query(self, query):
        conn = self._get_connection()
        try:
            return pd.read_sql_query(query, conn)
        finally:
            conn.close()
