import pymysql
from tqdm import tqdm

#전처리용 mysql에 접속하여 빈데이터와 10명이 되지 않는 데이터는 삭제







def connect_mysql(db='mydb'):
    conn = pymysql.connect(host='localhost', port=3306,
                           user='root', password='1234',
                           db=db, charset='utf8')
    return conn


def sql_execute(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def deleteData(conn) :
    query = 'select * from lol_datas' 
    return sql_execute(conn,query)