import google.generativeai as genai
import pandas as pd
from src.boxoffice.logic.sqlite_connector import SQLiteConnector
from src.boxoffice.logic.config import GeminiConfig
import re
import logging

logger = logging.getLogger(__name__)

class AIAgent:
    def __init__(self):
        """AI 에이전트를 초기화합니다."""
        try:
            gemini_config = GeminiConfig()
            genai.configure(api_key=gemini_config.api_key)
        except Exception as e:
            logger.error(f"Gemini API 키 설정 실패: {e}")
            raise ValueError("Gemini API 키를 설정해주세요. config/config.yml 파일을 확인하세요.") from e

        self.model = genai.GenerativeModel('gemini-1.5-flash-latest')
        self.db = SQLiteConnector()
        self.schema = self._get_db_schema()

    def _get_db_schema(self) -> str:
        """데이터베이스의 스키마 정보를 문자열로 반환합니다."""
        schema_info = []
        tables_df = self.db.select_query("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row['name'] for _, row in tables_df.iterrows() if not row['name'].startswith('sqlite_')]
        
        for table in tables:
            schema_info.append(f"테이블명: {table}")
            columns_df = self.db.select_query(f"PRAGMA table_info({table});")
            columns = [f"  - {row['name']} ({row['type']})" for _, row in columns_df.iterrows()]
            schema_info.append("\n".join(columns))
        
        return "\n\n".join(schema_info)

    def _generate_sql(self, question: str) -> str:
        """사용자의 질문을 기반으로 SQL 쿼리를 생성합니다."""
        prompt = f"""
        당신은 영화 데이터베이스 전문가입니다. 주어진 데이터베이스 스키마와 사용자의 질문을 바탕으로, 질문에 답할 수 있는 SQLite 쿼리문 하나만 생성해주세요.
        다른 설명이나 추가적인 텍스트 없이 오직 SQL 쿼리문만 반환해야 합니다.

        [데이터베이스 스키마]
        {self.schema}

        [규칙]
        - 날짜 관련 질문은 'YYYY-MM-DD' 형식을 사용하세요. (예: `date(targetDt)`)
        - `LIKE`를 사용할 때는 `%` 와일드카드를 적절히 사용하세요.
        - 생성된 쿼리는 SQLite에서 실행 가능해야 합니다.
        - 쿼리 외에 다른 말은 절대 하지 마세요.

        [사용자 질문]
        {question}

        [생성할 SQL 쿼리]
        """
        
        response = self.model.generate_content(prompt)
        sql_query = re.sub(r'```sql\n(.*?)\n```', r'\1', response.text, flags=re.DOTALL)
        return sql_query.replace('`', '').strip()

    def _get_answer_from_db(self, query: str) -> pd.DataFrame:
        """생성된 SQL 쿼리를 실행하여 결과를 반환합니다."""
        try:
            return self.db.select_query(query)
        except Exception as e:
            logger.error(f"SQL 쿼리 실행 실패: {query}, 오류: {e}")
            return pd.DataFrame([{"error": f"쿼리 실행 중 오류가 발생했습니다: {e}"}])

    def _summarize_result(self, question: str, query_result: pd.DataFrame) -> str:
        """쿼리 결과를 바탕으로 사용자에게 친절한 답변을 생성합니다."""
        if query_result.empty:
            return "해당 질문에 대한 데이터를 찾을 수 없습니다."
        
        if 'error' in query_result.columns:
            return f"죄송합니다, 질문을 처리하는 중 오류가 발생했습니다. 질문을 조금 더 명확하게 해주시겠어요?\n오류: {query_result['error'].iloc[0]}"

        prompt = f"""
        당신은 친절한 영화 데이터 분석가입니다. 사용자의 질문과 데이터베이스 조회 결과를 바탕으로, 자연스러운 한국어 문장으로 답변을 생성해주세요.
        표 형식의 데이터는 보기 좋게 마크다운 테이블로 만들어주세요.

        [사용자 질문]
        {question}

        [데이터베이스 조회 결과]
        {query_result.to_string()}

        [생성할 답변]
        """
        response = self.model.generate_content(prompt)
        return response.text

    def ask(self, question: str) -> dict:
        """사용자 질문에 대한 전체 프로세스를 실행하고 답변과 SQL을 반환합니다."""
        sql_query = self._generate_sql(question)
        query_result_df = self._get_answer_from_db(sql_query)
        answer = self._summarize_result(question, query_result_df)
        
        return {"answer": answer, "sql_query": sql_query}