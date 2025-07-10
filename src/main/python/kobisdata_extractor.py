import requests
import json
import pandas as pd
import numpy as np
import datetime
from src.main.python.config import KobisConfig

kobis_config = KobisConfig()

class KobisDataExtractor():
    def __init__(self):
        self.kobis_key = kobis_config.key

    def __get_extract_range(self, startDt_str, endDt_str):
        f = "%Y%m%d"
        startDt = datetime.datetime.strptime(startDt_str, f)
        endDt = datetime.datetime.strptime(endDt_str, f)
        extract_range = []
        extractDt = startDt
        while extractDt <= endDt:
            extractDt_str = extractDt.strftime(f)
            extract_range.append(extractDt_str)
            extractDt += datetime.timedelta(days=1)            
        return extract_range

    def __request_DailyBoxOffice(self, targetDt):
        # 일별 박스오피스
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
        params = {
            "key": self.kobis_key,
            "targetDt": targetDt
        }    
        while True:
            try:
                response = requests.get(url, params=params)
                text = response.text
                loads = json.loads(text)
                df = pd.DataFrame(loads["boxOfficeResult"]["dailyBoxOfficeList"])
            except:
                continue
            break
        return df
    
    def __request_MovieInfo(self, movieCd):
        # 영화 상세정보
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
        params = {
            "key": self.kobis_key,
            "movieCd": movieCd
        }
        while True:
            try:
                response = requests.get(url, params=params)
                text = response.text
                loads = json.loads(text)
                movie_info = loads["movieInfoResult"]["movieInfo"]
            except:
                continue
            break
        return movie_info

    def __request_MovieList(self, curPage, openStartDt, openEndDt):
        # 영화 목록
        url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"
        params = {
            "key": self.kobis_key,
            "itemPerPage": "100",
            "curPage": str(curPage),
            "openStartDt": openStartDt,
            "openEndDt": openEndDt
        }  
        list_exist = True
        while True:
            try:
                response = requests.get(url, params=params)
                text = response.text
                loads = json.loads(text)
                df = pd.DataFrame(loads["movieListResult"]["movieList"])
            except:
                continue
            break            
        if loads["movieListResult"]["totCnt"] == 0:
            list_exist = False 
        return df, list_exist

    def get_MovieList(self, start_year, end_year):
        movie_list = pd.DataFrame()
        for year in range(start_year, end_year+1):
            target_year = str(year)
            movie_y = pd.DataFrame()
            curPage = 1
            list_exist = True
            while list_exist:
                movie_p, list_exist = self.__request_MovieList(curPage, target_year, target_year)      
                movie_y = pd.concat([movie_y, movie_p], ignore_index=True)
                curPage += 1
            movie_y["directors"] = movie_y["directors"].apply(lambda x: [director["peopleNm"] for director in x])
            movie_y["directors_str"] = movie_y["directors"].astype(str)
            movie_y = movie_y[
                (movie_y["repGenreNm"]!="성인물(에로)") & 
                (movie_y["movieNmEn"]!="") &
                (movie_y["directors_str"]!="[]")].copy()
            movie_y = movie_y.drop(columns=["directors_str"])
            movie_list = pd.concat([movie_list, movie_y], ignore_index=True)
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

    def get_DailyBoxOffice(self, startDt_str, endDt_str):
        extract_range = self.__get_extract_range(startDt_str, endDt_str)
        boxoffice = pd.DataFrame()
        for extract_date in extract_range:
            df = self.__request_DailyBoxOffice(extract_date)
            df["targetDt"] = extract_date[:4] + "-" + extract_date[4:6] + "-" + extract_date[6:]
            boxoffice = pd.concat([boxoffice, df], ignore_index=True)
        boxoffice = boxoffice[boxoffice["openDt"]!=" "].copy()
        col_types = {
            "movieCd": "str",
            "salesAmt": "float",
            "salesShare": "float",
            "salesInten": "float",
            "salesChange": "float",
            "salesAcc": "float",
            "audiCnt": "float",
            "audiInten": "float",
            "audiChange": "float",
            "audiAcc": "float",
            "scrnCnt": "float",
            "showCnt": "float",
            "openDt": "datetime64[ns]",
            "targetDt": "datetime64[ns]",
        }
        boxoffice = boxoffice.astype(col_types)
        boxoffice["elapsedDt"] = (boxoffice["targetDt"] - boxoffice["openDt"]) / np.timedelta64(1, 'D')
        boxoffice["elapsedDt"] = boxoffice["elapsedDt"].astype(int)
        return boxoffice
    
    # def get_MovieBoxOffice(self, movieCd, period=None):
    #     movie_info = self.__request_MovieInfo(movieCd=movieCd)
    #     openDt = movie_info["openDt"]
    #     boxoffice = self.get_DailyBoxOffice(openDt, period)
    #     boxoffice = boxoffice[boxoffice["movieCd"]==movieCd].copy()
    #     boxoffice = boxoffice.reset_index(drop=True)
    #     return boxoffice

    # def get_MoviesBoxOffice(self, movieCds, period=None):
    #     boxoffice = pd.DataFrame()
    #     for movieCd in movieCds:
    #         df = self.get_MovieBoxOffice(movieCd=movieCd, period=period)
    #         boxoffice = pd.concat([boxoffice, df], ignore_index=True)
    #     return boxoffice
    
   

if __name__ == '__main__':
    kobisdata_extractor = KobisDataExtractor()
    # DailyBoxOffice = kobisdata_extractor.get_DailyBoxOffice("20231122", 15)
    # print("DailyBoxOffice")
    # print(DailyBoxOffice)

    MovieList = kobisdata_extractor.get_MovieList(2024, 2024)
    print("MovieList")
    print(MovieList.head(3))

    print(MovieList["openDt"].min())
    # print(df.head(5))
    # 서울의 봄: 20212866 / 슬램덩크: 20228555
    # movieCd = "20228555"
    # df = movie.get_MovieBoxOffice(movieCd)
    # print(df)
    # print(df[df['movieCd']=="20190549"]['movieNmEn'].values)
