########################################## 
###  This file creates data marts in the nyc_datamart db, then insertes data from the 
###  unified data table "warehouse_data"
##########################################

import psycopg2
from psycopg2 import sql


# Connection details for warehouse and datamart
mart_conn = psycopg2.connect(
    dbname="nyc_datamart",
    user="postgres",
    password="admin",
    host="localhost",
    port="15432"
)


def create_Marts():
    print('Creating Marts!')
    try:
        # Create Data Marts
        execute_sql_file(mart_conn, '../../sql/creation.sql')
        return 1
    except Exception as e:
        print(f"Problem occured : {e}")
        return 0



def insert_Marts():
    print('Inserting data to marts!')
    try:
        # Insert Data Marts With Data From "warehouse_data" table
        execute_sql_file(mart_conn, '../../sql/insertion.sql')
        mart_conn.close()
        return 1
    except Exception as e:
        print(f"Problem occured : {e}")
        return 0



def execute_sql_file(connection, file_path):
    with connection.cursor() as cursor, open(file_path, 'r') as sql_file:
        sql = sql_file.read()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        