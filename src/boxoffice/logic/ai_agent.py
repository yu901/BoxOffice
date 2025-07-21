import google.generativeai as genai
import pandas as pd
from .sqlite_connector import SQLiteConnector
from .config import GeminiConfig
import re
import logging
import json
from google.api_core.exceptions import ResourceExhausted

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
        
        [movie 테이블 스키마]
        movieCd	영화코드
        movieNm	영화명(국문)
        movieNmEn	영화명(영문)
        prdtYear	제작연도
        openDt	개봉일
        typeNm	영화유형
        prdtStatNm	제작상태
        nationAlt	제작국가(전체)
        genreAlt	영화장르(전체)
        repNationNm	대표 제작국가명
        repGenreNm	대표 장르명
        directors	영화감독
        peopleNm	영화감독명
        companys	제작사
        companyCd	제작사 코드
        companyNm	제작사명

        [boxoffice 테이블 스키마]
        boxofficeType	박스오피스 종류
        showRange	박스오피스 조회 일자
        rnum	순번
        rank	해당일자의 박스오피스 순위
        rankInten	전일대비 순위의 증감분
        rankOldAndNew	랭킹에 신규진입여부 (“OLD” : 기존 , “NEW” : 신규)
        movieCd	영화의 대표코드
        movieNm	영화명(국문)
        openDt	영화의 개봉일
        salesAmt	해당일의 매출액
        salesShare	해당일자 상영작의 매출총액 대비 해당 영화의 매출비율
        salesInten	전일 대비 매출액 증감분
        salesChange	전일 대비 매출액 증감 비율
        salesAcc	누적매출액
        audiCnt	해당일의 관객수
        audiInten	전일 대비 관객수 증감분
        audiChange	전일 대비 관객수 증감 비율
        audiAcc	누적관객수
        scrnCnt	해당일자에 상영한 스크린수
        showCnt	해당일자에 상영된 횟수
        targetDt	박스오피스 조회 일자

        [규칙]
        - 날짜 관련 질문은 'YYYY-MM-DD' 형식을 사용하세요. (예: `date(targetDt)`)
        - `LIKE`를 사용할 때는 `%` 와일드카드를 적절히 사용하세요.
        - 생성된 쿼리는 SQLite에서 실행 가능해야 합니다.
        - 쿼리 외에 다른 말은 절대 하지 마세요.
        - movie 테이블의 directors와 companys 컬럼은 JSON 형태의 문자열입니다. 이 컬럼들을 직접 쿼리하는 대신, 필요한 경우 `LIKE`와 `%`를 사용하여 JSON 문자열 내의 특정 값을 검색하세요. 예를 들어, 특정 감독의 영화를 찾으려면 `directors LIKE '%감독이름%'`과 같이 사용합니다.
        - `directors` 또는 `companys` 컬럼이 비어있는 리스트(`'[]'`)이거나 NULL인 경우는 집계에서 제외하세요. 예를 들어, `WHERE directors IS NOT NULL AND directors != '[]'`와 같이 조건을 추가하여 필터링합니다.
        - 사용자는 영화 제목을 정확하게 입력하지 않을 수 있습니다. 예를 들어, 사용자가 "어벤져스엔드게임"이라고 입력하면, 데이터베이스에 저장된 "어벤져스 엔드게임"과 비교해야 합니다. 따라서 쿼리에서 `movieNm`을 비교할 때는 `REPLACE(movieNm, ' ', '')` 함수를 사용하여 띄어쓰기를 제거한 후 `LIKE`와 `%`를 활용하여 검색하세요. **예시: `SELECT * FROM movie WHERE REPLACE(movieNm, ' ', '') LIKE '%어벤져스엔드게임%';`**
        - 사용자가 질문한 데이터 필드 외에 대표적인 필드 값도 같이 보여주세요. 예를 들어, 사용자가 "어벤져스 엔드게임의 감독은 누구인가요?"라고 질문하면, 쿼리 결과에 영화 제목과 감독 이름을 포함시켜야 합니다.
        - 사용자가 질문한 데이터 필드 값이 없다면 다른 필드 값이라도 보여주세요. 예를 들어, 사용자가 "가장 매출이 큰 영화의 제작사는?"라고 질문했을 때, 만약 제작사가 없다면 다른 필드 값(예: 가장 매출이 큰 영화의 영화제목과 매출액)을 보여주세요.

        [사용자 질문]
        {question}

        [생성할 SQL 쿼리]
        """
        
        try:
            response = self.model.generate_content(prompt)
            sql_query = re.sub(r'```sql\n(.*?)\n```', r'\1', response.text, flags=re.DOTALL)
            return sql_query.replace('`', '').strip()
        except ResourceExhausted:
            logger.error("Gemini API 할당량 초과: 일일 API 사용량을 모두 소진했습니다.")
            return "죄송합니다. 일일 API 사용량을 모두 소진했습니다. 내일 다시 시도해주세요."
        except Exception as e:
            logger.error(f"SQL 생성 중 예기치 않은 오류 발생: {e}")
            return f"죄송합니다. SQL 생성 중 오류가 발생했습니다: {e}."

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

        # directors와 companys 컬럼을 사람이 읽기 쉬운 형태로 변환
        display_result = query_result.copy()
        if 'directors' in display_result.columns:
            display_result['directors'] = display_result['directors'].apply(lambda x: ', '.join(json.loads(x)) if pd.notna(x) and x != '[]' else '')
        if 'companys' in display_result.columns:
            display_result['companys'] = display_result['companys'].apply(lambda x: ', '.join([c['companyNm'] for c in json.loads(x)]) if pd.notna(x) and x != '[]' else '')

        prompt = f"""
        당신은 친절한 영화 데이터 분석가입니다. 사용자의 질문과 데이터베이스 조회 결과를 바탕으로, 자연스러운 한국어 문장으로 답변을 생성해주세요.
        표 형식의 데이터는 보기 좋게 마크다운 테이블로 만들어주세요.

        [사용자 질문]
        {question}

        [데이터베이스 조회 결과]
        {display_result.to_string()}

        [생성할 답변]
        """
        response = self.model.generate_content(prompt)
        return response.text

    def ask(self, question: str) -> dict:
        """사용자 질문에 대한 전체 프로세스를 실행하고 답변과 SQL을 반환합니다."""
        try:
            sql_query = self._generate_sql(question)
        except ResourceExhausted:
            logger.error("Gemini API 할당량 초과: 일일 API 사용량을 모두 소진했습니다.")
            return {"answer": "죄송합니다. 일일 API 사용량을 모두 소진했습니다. 내일 다시 시도해주세요.", "sql_query": ""}
        except Exception as e:
            logger.error(f"SQL 생성 중 예기치 않은 오류 발생: {e}")
            return {"answer": f"죄송합니다. SQL 생성 중 오류가 발생했습니다: {e}.", "sql_query": ""}

        # SQL 쿼리 생성 단계에서 오류 메시지가 반환된 경우 즉시 처리 (이전 _generate_sql에서 반환된 경우)
        if sql_query.startswith("죄송합니다.") and "API 사용량을 모두 소진했습니다." in sql_query:
            return {"answer": sql_query, "sql_query": ""}

        query_result_df = self._get_answer_from_db(sql_query)
        answer = self._summarize_result(question, query_result_df)
        
        return {"answer": answer, "sql_query": sql_query}