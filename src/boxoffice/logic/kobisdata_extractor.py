import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
from src.boxoffice.logic.config import KobisConfig

kobis_config = KobisConfig()
logger = logging.getLogger(__name__)


class KobisDataExtractor():
    def __init__(self):
        self.kobis_key = kobis_config.key

    def __get_extract_range(self, startDt_str, endDt_str):
        f = "%Y%m%d"
        startDt = datetime.strptime(startDt_str, f)
        endDt = datetime.strptime(endDt_str, f)
        extract_range = []
        extractDt = startDt
        while extractDt <= endDt:
            extractDt_str = extractDt.strftime(f)
            extract_range.append(extractDt_str)
            extractDt += timedelta(days=1)
        return extract_range

    def __request_daily_boxoffice(self, target_dt: str) -> pd.DataFrame:
        # 일별 박스오피스
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
        params = {
            "key": self.kobis_key, "targetDt": target_dt
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data["boxOfficeResult"]["dailyBoxOfficeList"])
        except requests.RequestException as e:
            logger.error(f"일별 박스오피스 API 요청 실패 (날짜: {target_dt}): {e}")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"일별 박스오피스 응답 파싱 실패 (날짜: {target_dt}): {e}")
        return pd.DataFrame()

    def __request_movie_info(self, movie_cd: str) -> dict:
        # 영화 상세정보
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
        params = {
            "key": self.kobis_key, "movieCd": movie_cd
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data["movieInfoResult"]["movieInfo"]
        except requests.RequestException as e:
            logger.error(f"영화 상세정보 API 요청 실패 (코드: {movie_cd}): {e}")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"영화 상세정보 응답 파싱 실패 (코드: {movie_cd}): {e}")
        return {}

    def __request_movie_list(self, cur_page: int, year: str) -> (pd.DataFrame, bool):
        # 영화 목록
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"
        params = {
            "key": self.kobis_key,
            "itemPerPage": "100",
            "curPage": str(cur_page),
            "openStartDt": year,
            "openEndDt": year
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            movie_list = data["movieListResult"]["movieList"]
            has_more = (data["movieListResult"]["totCnt"] > 0) and (len(movie_list) > 0)
            return pd.DataFrame(movie_list), has_more
        except requests.RequestException as e:
            logger.error(f"영화 목록 API 요청 실패 (페이지: {cur_page}, 연도: {year}): {e}")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"영화 목록 응답 파싱 실패 (페이지: {cur_page}, 연도: {year}): {e}")
        return pd.DataFrame(), False

    def get_MovieList(self, year: int) -> pd.DataFrame:
        """특정 연도의 전체 영화 목록을 가져옵니다."""
        all_movies_for_year = []
        cur_page = 1
        has_more_pages = True
        while has_more_pages:
            movies_page_df, has_more_pages = self.__request_movie_list(cur_page, str(year))
            if not movies_page_df.empty:
                all_movies_for_year.append(movies_page_df)
            cur_page += 1

        if not all_movies_for_year:
            return pd.DataFrame()

        movie_list = pd.concat(all_movies_for_year, ignore_index=True)

        # 데이터 정제
        movie_list["directors"] = movie_list["directors"].apply(
            lambda directors: [d.get("peopleNm") for d in directors if d.get("peopleNm")]
        )
        
        # 필터링
        is_not_adult = movie_list["repGenreNm"] != "성인물(에로)"
        has_eng_title = movie_list["movieNmEn"].str.strip() != ""
        has_directors = movie_list["directors"].apply(len) > 0
        movie_list = movie_list[is_not_adult & has_eng_title & has_directors].copy()

        col_types = {
            "movieCd": "str",
            "movieNm": "str",
            "movieNmEn": "str",
            "prdtYear": "str",
            "openDt": "int",
            "typeNm": "str",
            "prdtStatNm": "str",
            "nationAlt": "str",
            "genreAlt": "str",
            "repNationNm": "str",
            "repGenreNm": "str",
            "directors": "object",
            "companys": "object",
        }
        movie_list = movie_list.astype(col_types)
        return movie_list

    def get_DailyBoxOffice(self, target_dt: datetime) -> pd.DataFrame:
        """특정 날짜의 일일 박스오피스 데이터를 가져옵니다."""
        if not isinstance(target_dt, datetime):
            raise TypeError("target_dt는 datetime 객체여야 합니다.")

        target_dt_str = target_dt.strftime("%Y%m%d")
        boxoffice_df = self.__request_daily_boxoffice(target_dt_str)

        if boxoffice_df.empty:
            return pd.DataFrame()

        boxoffice_df["targetDt"] = pd.to_datetime(target_dt.date())
        boxoffice_df["openDt"] = pd.to_datetime(boxoffice_df["openDt"], errors='coerce')
        boxoffice_df = boxoffice_df.dropna(subset=['openDt'])

        col_types = {
            col: "float" for col in boxoffice_df.columns if "Cnt" in col or "Amt" in col or "Share" in col or "Inten" in col or "Change" in col or "Acc" in col
        }
        boxoffice_df = boxoffice_df.astype(col_types)
        boxoffice_df["elapsedDt"] = (boxoffice_df["targetDt"] - boxoffice_df["openDt"]).dt.days
        return boxoffice_df

if __name__ == '__main__':
    kobisdata_extractor = KobisDataExtractor()
    # DailyBoxOffice = kobisdata_extractor.get_DailyBoxOffice("20231122", 15)
    # print("DailyBoxOffice")
    # print(DailyBoxOffice)

    MovieList = kobisdata_extractor.get_MovieList(2024)
    print("MovieList")
    print(MovieList.head(3))

    print(MovieList["openDt"].min())
