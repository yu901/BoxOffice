from supabase import create_client, Client
from src.boxoffice.logic.config import SupabaseConfig
from src.boxoffice.logic.base_connector import BaseDatabaseConnector
import pandas as pd
from typing import List, Dict

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
            # on_conflict 파라미터를 사용하여 중복 발생 시 업데이트를 수행합니다.
            response = self.client.table(table_name).upsert(data, on_conflict=conflict_column).execute()
            print(f"Successfully upserted {len(data)} rows to '{table_name}'.")
            return response
        except Exception as e:
            print(f"An error occurred during upsert: {e}")
            return None

    def insert_boxoffice(self, df: pd.DataFrame):
        if not df.empty:
            self._upsert_data('boxoffice', df.to_dict(orient='records'), 'movieCd,targetDt')

    def insert_movie(self, df: pd.DataFrame):
        if not df.empty:
            self._upsert_data('movie', df.to_dict(orient='records'), 'movieCd')

    def insert_goods_event(self, events: List[Dict]):
        if events:
            self._upsert_data('goods_event', events, 'event_id')

    def insert_goods_stock(self, df: pd.DataFrame):
        if not df.empty:
            self._upsert_data('goods_stock', df.to_dict(orient='records'), 'event_id,theater_name,scraped_at')

    def select_query(self, query: str) -> pd.DataFrame:
        # Supabase는 SQL 쿼리를 직접 실행하는 대신, 테이블 API를 사용합니다.
        # 따라서 이 메서드는 쿼리 문자열을 파싱하거나, 특정 테이블에 대한
        # 일반적인 select 로직을 구현해야 합니다.
        # 여기서는 간단한 예시로, 'SELECT * FROM table_name' 형태의 쿼리만 지원한다고 가정합니다.
        # 실제 사용 시에는 더 복잡한 쿼리 파싱 로직이 필요할 수 있습니다.
        try:
            table_name = query.split('FROM')[1].strip().split(' ')[0]
            response = self.client.table(table_name).select('*').execute()
            return pd.DataFrame(response.data)
        except Exception as e:
            print(f"An error occurred during select_query: {e}")
            return pd.DataFrame()
