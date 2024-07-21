import pymysql
import sqlalchemy
from config import MysqlConfig    

class MySQLConnector:
    def __init__(self):
        self.config = MysqlConfig()
        self.create_query = MySQLCreateQuery(self)
        self.delete_query = MySQLDeleteQuery(self)
        self.insert_query = MySQLInsertQuery(self)
        
class MySQLCreateQuery:
    def __init__(self, info: MySQLConnector):
        config = info.config
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
                    `openDt` DATE,
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
                    `targetDt` DATE,
                    `elapsedDt` int DEFAULT NULL
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
                    `prdtYear` VARCHAR(4) DEFAULT NULL,
                    `openDt` DATE,
                    `typeNm` VARCHAR(10),
                    `prdtStatNm` VARCHAR(5),
                    `nationAlt` text,
                    `genreAlt` text,
                    `repNationNm` text,
                    `repGenreNm` VARCHAR(10),
                    `directors` JSON,
                    `companys` JSON
                );
            """
        self.cursor.execute(query)
        return f"create table {table_name} complete."

class MySQLDeleteQuery:
    def __init__(self, info: MySQLConnector):
        config = info.config
        self.conn = pymysql.connect(
            user = config.user,
            passwd = config.password,
            host = config.host,
            db = config.database
        )

    def delete_boxoffice(self, start_at, end_at):
        self.cursor = self.conn.cursor()
        table_name = f"boxoffice"
        query = \
            f"""
                DELETE FROM`{table_name}`
                WHERE targetDt >= '{start_at}' and targetDt <= '{end_at}';
            """
        self.cursor.execute(query)
        self.conn.commit()
        self.cursor.close()
        return f"delete table {table_name} complete."

    def delete_movie(self, start_at, end_at):
        self.cursor = self.conn.cursor()
        table_name = f"movie"
        query = \
            f"""
                DELETE FROM `{table_name}`
                WHERE openDt >= {start_at} and openDt <= {end_at};
            """
        self.cursor.execute(query)
        self.conn.commit()
        self.cursor.close()
        return f"create table {table_name} complete."

class MySQLInsertQuery:
    def __init__(self, info: MySQLConnector):
        self.info = info
        config = self.info.config
        connection_string = f"mysql+pymysql://{config.user}:{config.password}@{config.host}/{config.database}"
        self.engine = sqlalchemy.create_engine(connection_string)#, fast_executemany=True)

    def insert_boxoffice(self, df):
        self.info.delete_query.delete_boxoffice(df["targetDt"].min(), df["targetDt"].max())
        self.conn = self.engine.connect()
        df.to_sql(
            name="boxoffice", 
            con=self.engine, 
            if_exists='append', 
            index=False)
        self.conn.close()

    def insert_movie(self, df):
        self.info.delete_query.delete_movie(df["openDt"].min(), df["openDt"].max())
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
    db_conn.insertor.movie(MovieList)
