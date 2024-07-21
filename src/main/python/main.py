from kobisdata_extractor import KobisDataExtractor
from mysql_connector import MySQLConnector

kobis_extractor = KobisDataExtractor()
db_connector = MySQLConnector()

# movie_list = kobis_extractor.get_MovieList(2010, 2024)
# db_connector.insert_query.insert_movie(movie_list)

# daily_boxoffice = kobis_extractor.get_DailyBoxOffice("20240101", "20240721")
# db_connector.insert_query.insert_boxoffice(daily_boxoffice)