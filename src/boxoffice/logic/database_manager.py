from .config import DatabaseConfig
from .sqlite_connector import SQLiteConnector
from .supabase_connector import SupabaseConnector
from .base_connector import BaseDatabaseConnector

def get_database_connector() -> BaseDatabaseConnector:
    """
    secrets.toml의 database.type 설정에 따라 적절한 데이터베이스 커넥터 인스턴스를 반환합니다.
    """
    config = DatabaseConfig()
    db_type = config.type.lower()

    if db_type == "sqlite":
        return SQLiteConnector()
    elif db_type == "supabase":
        return SupabaseConnector()
    else:
        raise ValueError(f"Unsupported database type: {db_type}. Must be 'sqlite' or 'supabase'.")
