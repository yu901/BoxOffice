from src.main.python.kobisdata_extractor import KobisDataExtractor
from src.main.python.sqlite_connector import SQLiteConnector

if __name__ == "__main__":
    extractor = KobisDataExtractor()
    db = SQLiteConnector()

    # 1. 박스오피스 데이터 (최근 일주일)
    box_df = extractor.get_DailyBoxOffice("20250701", "20250704")
    print(f"박스오피스 데이터: {len(box_df)}건")
    db.insert_boxoffice(box_df)

    # 2. 영화 목록 데이터 (2025년)
    movie_df = extractor.get_MovieList(2025, 2025)
    print(f"영화 목록 데이터: {len(movie_df)}건")
    db.insert_movie(movie_df)

    # 3. 쿼리 확인
    result = db.select_query("SELECT movieNm, openDt FROM movie ORDER BY openDt desc LIMIT 5 ;")
    print(result)
