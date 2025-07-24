from supabase import create_client, Client
from src.boxoffice.logic.config import SupabaseConfig
from src.boxoffice.logic.base_connector import BaseDatabaseConnector
import pandas as pd
from typing import List, Dict
from datetime import datetime, date # datetime과 date import 추가

class SupabaseConnector(BaseDatabaseConnector):
    def __init__(self):
        self.config = SupabaseConfig()
        self.client = self._get_supabase_client()

    def _get_supabase_client(self) -> Client:
        """
        Supabase 클라이언트를 생성하고 반환합니다.
        SupabaseConfig를 통해 Supabase URL과 service_role 키를 가져옵니다.
        """
        url: str = self.config.url
        key: str = self.config.service_role_key
        
        if not url or not key or "YOUR_SUPABASE_URL" in url or "YOUR_SUPABASE_SERVICE_ROLE_KEY" in key:
            raise ValueError("Supabase URL and service_role key must be set correctly in secrets.toml.")
            
        return create_client(url, key)

    def _upsert_data(self, table_name: str, data: list[dict], conflict_column: str):
        """
        Supabase 테이블에 데이터를 upsert합니다.

        :param table_name: 데이터를 삽입할 테이블 이름
        :param data: 삽입할 데이터 (딕셔너리의 리스트)
        :param conflict_column: 중복 확인의 기준이 될 컬럼명
        """
        try:
            processed_data = []
            for record in data:
                processed_record = {}
                for k, v in record.items():
                    # datetime 객체를 ISO 8601 문자열로 변환
                    if isinstance(v, (datetime, date)):
                        processed_record[self._get_db_column_name(k)] = v.isoformat()
                    elif pd.isna(v):
                        processed_record[self._get_db_column_name(k)] = None # NaN 값을 None으로 명시적으로 변환
                    elif self._get_db_column_name(k) == 'total_quantity' and isinstance(v, float):
                        processed_record[self._get_db_column_name(k)] = int(v) # float 형태의 정수를 int로 변환
                    else:
                        processed_record[self._get_db_column_name(k)] = v
                processed_data.append(processed_record)

            # conflict_column을 소문자로 변환
            db_conflict_column = ','.join([col.strip().lower() for col in conflict_column.split(',')])
            response = self.client.table(table_name).upsert(processed_data, on_conflict=db_conflict_column).execute()
            return response
        except Exception as e:
            print(f"An error occurred during upsert: {e}")
            return None

    def insert_boxoffice(self, df: pd.DataFrame):
        print(f"[SupabaseConnector] Attempting to insert {len(df)} rows into boxoffice.")
        if not df.empty:
            self._upsert_data('boxoffice', df.to_dict(orient='records'), 'movie_cd,target_dt')

    def insert_movie(self, df: pd.DataFrame):
        print(f"[SupabaseConnector] Attempting to insert {len(df)} rows into movie.")
        if not df.empty:
            self._upsert_data('movie', df.to_dict(orient='records'), 'movie_cd')

    def insert_goods_event(self, events: List[Dict]):
        print(f"[SupabaseConnector] Attempting to insert {len(events)} rows into goods_event.")
        if events:
            self._upsert_data('goods_event', events, 'event_id')

    def insert_goods_stock(self, df: pd.DataFrame):
        print(f"[SupabaseConnector] Attempting to insert {len(df)} rows into goods_stock.")
        if not df.empty:
            df.columns = [self._get_db_column_name(col) for col in df.columns]
            # quantity 컬럼의 NaN 값을 빈 문자열로 변환
            if 'quantity' in df.columns:
                df['quantity'] = df['quantity'].fillna("")
            
            # total_quantity 컬럼의 NaN 값을 None으로 변환
            if 'total_quantity' in df.columns:
                df['total_quantity'] = df['total_quantity'].where(pd.notna(df['total_quantity']), None)

            self._upsert_data('goods_stock', df.to_dict(orient='records'), 'event_id,theater_name,scraped_at')

    def select_query(self, sql: str) -> pd.DataFrame:
        try:
            # RPC를 호출하여 SQL 실행
            response = self.client.rpc('execute_sql', {'sql_query': sql}).execute()
            
            # 응답 데이터 처리
            if response.data:
                return pd.DataFrame(response.data)
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"An error occurred during select_query: {e}")
            return pd.DataFrame()

    def _get_db_column_name(self, logical_name: str) -> str:
        """논리적 컬럼 이름을 Supabase DB의 실제 컬럼 이름(소문자)으로 변환합니다."""
        return logical_name.lower()
