import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
from .config import KobisConfig

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
        return pd.DataFrame(data) if data else pd.DataFrame()

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
        return pd.DataFrame(movie_list), has_more

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
        # Process directors
        def process_directors(directors_list):
            if isinstance(directors_list, list):
                # Use ensure_ascii=False to store actual Korean characters
                return json.dumps([d.get("peopleNm") for d in directors_list if d.get("peopleNm")] or [], ensure_ascii=False)
            return "[]" # Default to empty JSON array if not a list

        movie_list["directors"] = movie_list["directors"].apply(process_directors)

        # Process companys
        def process_companys(companys_list):
            if isinstance(companys_list, list):
                # Use ensure_ascii=False to store actual Korean characters
                return json.dumps([{'companyCd': c.get('companyCd'), 'companyNm': c.get('companyNm')} for c in companys_list if c.get('companyCd') and c.get('companyNm')] or [], ensure_ascii=False)
            return "[]" # Default to empty JSON array if not a list

        movie_list["companys"] = movie_list["companys"].apply(process_companys)

        is_not_adult = movie_list["repGenreNm"] != "성인물(에로)"
        has_eng_title = movie_list["movieNmEn"].str.strip() != ""
        has_directors = movie_list["directors"].apply(lambda x: len(json.loads(x)) > 0 if x else False)
        movie_list = movie_list[is_not_adult & has_eng_title & has_directors].copy()

        # openDt를 YYYY-MM-DD 형식의 문자열로 변환
        movie_list["openDt"] = pd.to_datetime(movie_list["openDt"], errors='coerce').dt.strftime('%Y-%m-%d')

        col_types = {
            "movieCd": "str", "movieNm": "str", "movieNmEn": "str", "prdtYear": "str",
            "openDt": "str", "typeNm": "str", "prdtStatNm": "str", "nationAlt": "str",
            "genreAlt": "str", "repNationNm": "str", "repGenreNm": "str",
            "directors": "str", "companys": "str",
        }
        movie_list = movie_list.astype(col_types)
        return movie_list

    def get_DailyBoxOffice(self, target_dt: datetime) -> pd.DataFrame:
        if not isinstance(target_dt, datetime):
            raise TypeError("target_dt는 datetime 객체여야 합니다.")

        target_dt_str = target_dt.strftime("%Y%m%d")
        boxoffice_df = self._request_daily_boxoffice(target_dt_str)
        if boxoffice_df.empty:
            return pd.DataFrame()

        boxoffice_df["targetDt"] = pd.to_datetime(target_dt.date())
        boxoffice_df["openDt"] = pd.to_datetime(boxoffice_df["openDt"], errors='coerce')
        boxoffice_df = boxoffice_df.dropna(subset=['openDt'])

        col_types = {
            "rnum": "int",
            "rank": "int",
            "rankInten": "int",
            "salesAmt": "float",
            "salesShare": "float",
            "salesInten": "float",
            "salesChange": "float",
            "audiCnt": "float",
            "audiInten": "float",
            "audiChange": "float",
            "audiAcc": "float",
            "scrnCnt": "float",
            "showCnt": "float",
        }
        for col, dtype in col_types.items():
            if col in boxoffice_df.columns:
                if dtype == "int":
                    boxoffice_df[col] = pd.to_numeric(boxoffice_df[col], errors='coerce').fillna(0).astype(int)
                elif dtype == "float":
                    boxoffice_df[col] = pd.to_numeric(boxoffice_df[col], errors='coerce').fillna(0).astype(float)

        boxoffice_df["elapsed_dt"] = (boxoffice_df["target_dt"] - boxoffice_df["open_dt"]).dt.days
        return boxoffice_df


