import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
from src.boxoffice.logic.config import KobisConfig

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
        movie_list["directors"] = movie_list["directors"].apply(
            lambda directors: [d.get("peopleNm") for d in directors if d.get("peopleNm")]
        )
        is_not_adult = movie_list["repGenreNm"] != "성인물(에로)"
        has_eng_title = movie_list["movieNmEn"].str.strip() != ""
        has_directors = movie_list["directors"].apply(len) > 0
        movie_list = movie_list[is_not_adult & has_eng_title & has_directors].copy()

        col_types = {
            "movieCd": "str", "movieNm": "str", "movieNmEn": "str", "prdtYear": "str",
            "openDt": "int", "typeNm": "str", "prdtStatNm": "str", "nationAlt": "str",
            "genreAlt": "str", "repNationNm": "str", "repGenreNm": "str",
            "directors": "object", "companys": "object",
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
            col: "float" for col in boxoffice_df.columns
            if any(x in col for x in ["Cnt", "Amt", "Share", "Inten", "Change", "Acc"])
        }
        boxoffice_df = boxoffice_df.astype(col_types)
        boxoffice_df["elapsedDt"] = (boxoffice_df["targetDt"] - boxoffice_df["openDt"]).dt.days
        return boxoffice_df

if __name__ == '__main__':
    kobisdata_extractor = KobisDataExtractor()
    MovieList = kobisdata_extractor.get_MovieList(2024)
    print("MovieList")
    print(MovieList.head(3))
    print(MovieList["openDt"].min())
