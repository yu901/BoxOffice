import pymysql
from config import MysqlConfig    

class MySQLConnector:
    def __init__(self):
        config = MysqlConfig()
        self.conn = pymysql.connect(
            user = config.user,
            passwd = config.password,
            host = config.host,
            db = config.database
        )
        self.cursor = self.conn.cursor()
        
    def __del__(self):
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    mysql_connector = MySQLConnector()
    mysql_connector.cursor.execute("SELECT * FROM movie_list;")
    select_all_result = mysql_connector.cursor.fetchall()  
    print(select_all_result)