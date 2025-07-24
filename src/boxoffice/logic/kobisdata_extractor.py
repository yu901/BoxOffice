import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
from .config import KobisConfig
from .utils import camel_to_snake, convert_dict_keys_snake_case, infer_col_types_from_df, auto_cast_dataframe

kobis_config = KobisConfig()
logger = logging.getLogger(__name__)

class KobisDataExtractor:
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/"

    def __init__(self):
        self.kobis_key = kobis_config.key

    def _request_api(self, endpoint: str, params: dict, data_path: list = None) -> dict:
        url = self.base_url + endpoint
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data_path:
                for key in data_path:
                    data = data[key]
            return data
        except requests.RequestException as e:
            logger.error(f"API 요청 실패 ({endpoint}): {e}")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"API 응답 파싱 실패 ({endpoint}): {e}")
        return {}

    def _request_daily_boxoffice(self, target_dt: str) -> pd.DataFrame:
        params = {"key": self.kobis_key, "targetDt": target_dt}
        data = self._request_api(
            "boxoffice/searchDailyBoxOfficeList.json",
            params,
            ["boxOfficeResult", "dailyBoxOfficeList"]
        )
        if not data:
            return pd.DataFrame()

        normalized_list = [convert_dict_keys_snake_case(item) for item in data]

        return pd.DataFrame(normalized_list)

    def _request_movie_info(self, movie_cd: str) -> dict:
        params = {"key": self.kobis_key, "movieCd": movie_cd}
        return self._request_api(
            "movie/searchMovieInfo.json",
            params,
            ["movieInfoResult", "movieInfo"]
        )

    def _request_movie_list(self, cur_page: int, year: str) -> tuple[pd.DataFrame, bool]:
        params = {
            "key": self.kobis_key,
            "itemPerPage": "100",
            "curPage": str(cur_page),
            "openStartDt": year,
            "openEndDt": year
        }
        data = self._request_api(
            "movie/searchMovieList.json",
            params,
            ["movieListResult"]
        )
        movie_list = data.get("movieList", []) if data else []
        has_more = bool(movie_list)
        snake_movie_list = [
            {camel_to_snake(k): v for k, v in movie.items()}
            for movie in movie_list
        ]
        return pd.DataFrame(snake_movie_list), has_more

    def get_MovieList(self, year: int) -> pd.DataFrame:
        all_movies_for_year = []
        cur_page = 1
        while True:
            movies_page_df, has_more = self._request_movie_list(cur_page, str(year))
            if movies_page_df.empty:
                break
            all_movies_for_year.append(movies_page_df)
            cur_page += 1
            if not has_more:
                break

        if not all_movies_for_year:
            return pd.DataFrame()

        movie_list = pd.concat(all_movies_for_year, ignore_index=True)
        def process_directors(directors_list):
            if isinstance(directors_list, list):
                snake_list = [convert_dict_keys_snake_case(d) for d in directors_list]
                return json.dumps(
                    [d.get("people_nm") for d in snake_list if d.get("people_nm")] or [],
                    ensure_ascii=False
                )
            return "[]"

        def process_companys(companys_list):
            if isinstance(companys_list, list):
                snake_list = [convert_dict_keys_snake_case(c) for c in companys_list]
                return json.dumps(
                    [{'company_cd': c.get('company_cd'), 'company_nm': c.get('company_nm')} for c in snake_list if c.get('company_cd') and c.get('company_nm')] or [],
                    ensure_ascii=False
                )
            return "[]"

        # directors 처리 추가
        movie_list["directors"] = movie_list["directors"].apply(process_directors)
        movie_list["companys"] = movie_list["companys"].apply(process_companys)

        is_not_adult = movie_list["rep_genre_nm"] != "성인물(에로)"
        has_eng_title = movie_list["movie_nm_en"].astype(str).str.strip() != ""
        has_directors = movie_list["directors"].apply(lambda x: len(json.loads(x)) > 0 if x else False)
        movie_list = movie_list[is_not_adult & has_eng_title & has_directors].copy()

        # open_dt를 YYYY-MM-DD 형식의 문자열로 변환
        movie_list["open_dt"] = pd.to_datetime(movie_list["open_dt"], errors='coerce').dt.strftime('%Y-%m-%d')

        col_types = infer_col_types_from_df(movie_list)
        movie_list = movie_list.astype(col_types)
        return movie_list

    def get_DailyBoxOffice(self, target_dt: datetime) -> pd.DataFrame:
        if not isinstance(target_dt, datetime):
            raise TypeError("target_dt는 datetime 객체여야 합니다.")

        target_dt_str = target_dt.strftime("%Y%m%d")
        boxoffice_df = self._request_daily_boxoffice(target_dt_str)
        if boxoffice_df.empty:
            return pd.DataFrame()

        boxoffice_df["target_dt"] = pd.to_datetime(target_dt.date())
        boxoffice_df["open_dt"] = pd.to_datetime(boxoffice_df["open_dt"], errors='coerce')
        boxoffice_df = boxoffice_df.dropna(subset=['open_dt'])

        col_types = infer_col_types_from_df(boxoffice_df)
        boxoffice_df = auto_cast_dataframe(boxoffice_df, col_types)

        # 2. 날짜 컬럼만 별도로 datetime으로 변환
        for date_col in ["target_dt", "open_dt"]:
            if date_col in boxoffice_df.columns:
                boxoffice_df[date_col] = pd.to_datetime(boxoffice_df[date_col], errors='coerce')

        # 3. 날짜 차이 계산
        boxoffice_df["elapsed_dt"] = (boxoffice_df["target_dt"] - boxoffice_df["open_dt"]).dt.days
        return boxoffice_df

if __name__ == '__main__':
    kobisdata_extractor = KobisDataExtractor()
    df = kobisdata_extractor.get_MovieList(2024)
    print(df.head(3))

    target_dt = datetime(2025, 7, 20)
    df = kobisdata_extractor.get_DailyBoxOffice(target_dt)
    print(df.head())