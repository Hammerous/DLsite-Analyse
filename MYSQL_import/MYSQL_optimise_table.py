import pymysql
db_name = 'doujinDB'

if __name__=='__main__':
    db = pymysql.connect(host='localhost', user='root', password='Pathfinder', charset='utf8')
    cursor = db.cursor()
    cursor.execute("USE {0}".format(db_name))
    cursor.execute('SHOW TABLES;')
    tables = cursor.fetchall()
    for table_name in tables:
        print(f"Optimising: {table_name[0]} ...")
        cursor.execute(f'OPTIMIZE TABLE {table_name[0]};')
    cursor.close()
    db.close()