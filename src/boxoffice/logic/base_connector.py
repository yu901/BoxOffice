from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict

class BaseDatabaseConnector(ABC):
    """모든 데이터베이스 커넥터가 구현해야 할 추상 기본 클래스"""

    @abstractmethod
    def insert_boxoffice(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def insert_movie(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def select_query(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def insert_goods_event(self, events: List[Dict]):
        pass

    @abstractmethod
    def insert_goods_stock(self, stocks: pd.DataFrame):
        pass

    @abstractmethod
    def _get_db_column_name(self, logical_name: str) -> str:
        pass
