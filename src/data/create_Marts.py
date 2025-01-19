########################################## 
###  This file creates data marts in the nyc_datamart db, 
###  then insertes data from the Warehouse
##########################################

from connection_config import connect_Datamart


def create_Marts():
    print("\033[1;32m        ########    Creating Marts!\033[0m")
    try:
        mart_conn = connect_Datamart()
        execute_sql_file(mart_conn, '../../sql/creation.sql')
        return 1
    except Exception as e:
        print("\033[1;31m        ######    Problem Occured While Creating Marts ######\033[0m")
        print(e)
        return 0



def insert_Marts():
    print("\033[1;32m        ########    Inserting Data To Marts!\033[0m")

    try:
        mart_conn = connect_Datamart()
        execute_sql_file(mart_conn, '../../sql/insertion.sql')
        mart_conn.close()
        return 1
    except Exception as e:
        print("\033[1;31m        ########    Problem Occured While Inserting Data To Marts!\033[0m")
        print(e)
        return 0



def execute_sql_file(connection, file_path):
    with connection.cursor() as cursor, open(file_path, 'r') as sql_file:
        sql = sql_file.read()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        