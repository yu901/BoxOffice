import pymysql
import sqlalchemy
from config import MysqlConfig    

class MySQLConnector:
    def __init__(self):
        config = MysqlConfig()
        self.create = MySQLCreateQuery(config)
        self.insert = MySQLInsertQuery(config)
        
class MySQLCreateQuery:
    def __init__(self, config):
        self.conn = pymysql.connect(
            user = config.user,
            passwd = config.password,
            host = config.host,
            db = config.database
        )
        self.cursor = self.conn.cursor()
        self.__create_boxoffice_table()
        self.__create_movie_table()
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def __create_boxoffice_table(self):
        table_name = f"boxoffice"
        query = \
            f"""
                CREATE TABLE IF NOT EXISTS `{table_name}`
                (
                    `rnum` bigint DEFAULT NULL,
                    `rank` bigint DEFAULT NULL,
                    `rankInten` bigint DEFAULT NULL,
                    `rankOldAndNew` text,
                    `movieCd` text,
                    `movieNm` text,
                    `openDt` text,
                    `salesAmt` bigint DEFAULT NULL,
                    `salesShare` double DEFAULT NULL,
                    `salesInten` bigint DEFAULT NULL,
                    `salesChange` double DEFAULT NULL,
                    `salesAcc` bigint DEFAULT NULL,
                    `audiCnt` bigint DEFAULT NULL,
                    `audiInten` bigint DEFAULT NULL,
                    `audiChange` double DEFAULT NULL,
                    `audiAcc` bigint DEFAULT NULL,
                    `scrnCnt` bigint DEFAULT NULL,
                    `showCnt` bigint DEFAULT NULL,
                    `targetDt` text
                );
            """
        self.cursor.execute(query)
        return f"create table {table_name} complete."

    def __create_movie_table(self):
        table_name = f"movie"
        query = \
            f"""
                CREATE TABLE IF NOT EXISTS `{table_name}`
                (
                    `movieCd` text,
                    `movieNm` text,
                    `movieNmEn` text,
                    `prdtYear` bigint DEFAULT NULL,
                    `openDt` bigint DEFAULT NULL,
                    `typeNm` text,
                    `prdtStatNm` text,
                    `nationAlt` text,
                    `genreAlt` text,
                    `repNationNm` text,
                    `repGenreNm` text,
                    `directors` JSON,
                    `companys` JSON
                );
            """
        self.cursor.execute(query)
        return f"create table {table_name} complete."

class MySQLInsertQuery:
    def __init__(self, config):
        connection_string = f"mysql+pymysql://{config.user}:{config.password}@{config.host}/{config.database}"
        self.engine = sqlalchemy.create_engine(connection_string, fast_executemany=True)

    def boxoffice(self, df):
        self.conn = self.engine.connect()
        df.to_sql(
            name="boxoffice", 
            con=self.engine, 
            if_exists='append', 
            index=False)
        self.conn.close()

    def movie(self, df):
        self.conn = self.engine.connect()
        df.to_sql(
            name="movie", 
            con=self.engine, 
            if_exists='append', 
            index=False, 
            dtype={
                "directors": sqlalchemy.types.JSON, 
                "companys": sqlalchemy.types.JSON
                }
            )
        self.conn.close()

if __name__ == '__main__':
    from kobisdata_extractor import KobisDataExtractor

    kobisdata_extractor = KobisDataExtractor()
    MovieList = kobisdata_extractor.get_MovieList("2022", 1)

    db_conn = MySQLConnector()
    db_conn.insert.movie(MovieList)
