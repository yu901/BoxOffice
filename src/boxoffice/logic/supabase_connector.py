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
        """
        Executes a SQL query and returns the result as a pandas DataFrame.
        Handles PostgREST's 1000-row limit by paginating through the results
        for SELECT queries.
        """
        # non-SELECT 쿼리의 경우, 페이지네이션 없이 단일 RPC 호출을 사용합니다.
        if not sql.strip().lower().startswith('select'):
            try:
                response = self.client.rpc('execute_sql', {'sql_query': sql}).execute()
                if response.data:
                    return pd.DataFrame(response.data)
                return pd.DataFrame()
            except Exception as e:
                print(f"An error occurred during non-SELECT query: {e}")
                return pd.DataFrame()

        try:
            all_data = []
            offset = 0
            limit = 1000  # PostgREST 기본 제한

            clean_sql = sql.strip().rstrip(';')

            while True:
                # 페이지네이션을 위해 LIMIT과 OFFSET을 추가합니다.
                paginated_sql = f"{clean_sql} LIMIT {limit} OFFSET {offset}"
                
                response = self.client.rpc('execute_sql', {'sql_query': paginated_sql}).execute()
                
                # 더 이상 데이터가 없으면 루프를 종료합니다.
                if not response.data:
                    break
                
                all_data.extend(response.data)
                
                # 현재 페이지의 데이터가 limit보다 작으면 마지막 페이지입니다.
                if len(response.data) < limit:
                    break
                
                offset += limit
                
            return pd.DataFrame(all_data)

        except Exception as e:
            print(f"An error occurred during paginated select_query: {e}")
            return pd.DataFrame()

    def _get_db_column_name(self, logical_name: str) -> str:
        """논리적 컬럼 이름을 Supabase DB의 실제 컬럼 이름(소문자)으로 변환합니다."""
        return logical_name.lower()
