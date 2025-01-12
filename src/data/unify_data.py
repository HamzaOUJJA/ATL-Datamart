########################################## 
###  This file concatenates all warehouse tables into one in datamart.
##########################################


from connection_config import connect_Datamart



def execute_sql_file(connection, file_path):
    with connection.cursor() as cursor, open(file_path, 'r') as sql_file:
        sql = sql_file.read()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()

def unify_data():
    print('Merging data!')
    try:
        mart_conn = connect_Datamart()
        execute_sql_file(mart_conn, '../../sql/unify_data.sql')
        return 1
    except Exception as e:
        print("\033[1;31m ###### Problem Occured While Unifying Data In Datamart ######\033[0m")
        print(e)
        return 0