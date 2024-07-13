import requests
import json
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import urllib.request
import re
from config import KobisConfig
import ast

kobis_config = KobisConfig()

class KobisDataExtractor():
    def __init__(self):
        self.kobis_key = kobis_config.key

    def get_extract_range(self, startDt, period=None):
        f = "%Y%m%d"
        start_time = datetime.datetime.strptime(startDt, f)
        now_time = datetime.datetime.now() - datetime.timedelta(days=1)
        if period == None:
            limit_time = now_time
        else:
            limit_time = min(now_time, start_time + datetime.timedelta(days=period))
        extract_range = []
        extract_time = start_time
        while extract_time < limit_time:
            extract_str = extract_time.strftime(f)
            extract_range.append(extract_str)
            extract_time += datetime.timedelta(days=1)            
        return extract_range

    def request_DailyBoxOffice(self, targetDt):
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
    
    def request_MovieInfo(self, movieCd):
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

    def request_MovieList(self, curPage, openStartDt, openEndDt):
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

    def get_MovieList(self, openStartDt, period=1):
        movie_list = pd.DataFrame()
        target_year = openStartDt
        for years in range(period):
            target_year = str(int(target_year) + years)
            movie_y = pd.DataFrame()
            curPage = 1
            list_exist = True
            while list_exist:
                movie_p, list_exist = self.request_MovieList(curPage, target_year, target_year)      
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
        return movie_list

    def get_DailyBoxOffice(self, startDt, period=None):
        extract_range = self.get_extract_range(startDt, period)
        boxoffice = pd.DataFrame()
        for extract_date in extract_range:
            df = self.request_DailyBoxOffice(extract_date)
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
    
    def get_MovieBoxOffice(self, movieCd, period=None):
        movie_info = self.request_MovieInfo(movieCd=movieCd)
        openDt = movie_info["openDt"]
        boxoffice = self.get_DailyBoxOffice(openDt, period)
        boxoffice = boxoffice[boxoffice["movieCd"]==movieCd].copy()
        boxoffice = boxoffice.reset_index(drop=True)
        return boxoffice

    def get_MoviesBoxOffice(self, movieCds, period=None):
        boxoffice = pd.DataFrame()
        for movieCd in movieCds:
            df = self.get_MovieBoxOffice(movieCd=movieCd, period=period)
            boxoffice = pd.concat([boxoffice, df], ignore_index=True)
        return boxoffice
    
   

if __name__ == '__main__':
    kobisdata_extractor = KobisDataExtractor()
    DailyBoxOffice = kobisdata_extractor.get_DailyBoxOffice("20231122", 15)
    print("DailyBoxOffice")
    print(DailyBoxOffice)

    MovieList = kobisdata_extractor.get_MovieList("2022", 1)
    print("MovieList")
    print(MovieList)
    
    # print(df.head(5))
    # 서울의 봄: 20212866 / 슬램덩크: 20228555
    # movieCd = "20228555"
    # df = movie.get_MovieBoxOffice(movieCd)
    # print(df)
    # print(df[df['movieCd']=="20190549"]['movieNmEn'].values)
