from langchain_community.utilities import SQLDatabase
from langchain.chains.sql_database.query import create_sql_query_chain
from timeit import default_timer as timer
from dotenv import dotenv_values
from config import MysqlConfig
from langchain_groq import ChatGroq

mysql_config = MysqlConfig()
config = dotenv_values(".env")
api_key = config["GROQ_API_KEY"]

def get_msdb(table):
    db_uri = mysql_config.get_url() 
    return SQLDatabase.from_uri(db_uri, include_tables=[table])

# @retry(tries=5, delay=5)
def get_sql(table, question):
    start = timer()
    try:
        msdb = get_msdb(table)  
        query = __get_sql(question, msdb)    
        return query 
    except Exception as ex:
        print(ex)
    finally:
        print('Cypher Generation Time : {}'.format(timer() - start))


def __get_sql(question, msdb):
    llm = ChatGroq(
            model="mixtral-8x7b-32768",
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
        )
    chain = create_sql_query_chain(llm, msdb,)
    query = chain.invoke({"question" : question })
    return query 

if __name__ == '__main__':
    from mysql_connector import MySQLConnector

    # Q. What movie will have the highest advance ticket sales in 2024-01-01?
    question = input("Please enter your question.")
    sql = get_sql("boxoffice", question)
    print(sql)

    mysql_conn = MySQLConnector()
    result = mysql_conn.select_query.get_result(sql)
    print(result)