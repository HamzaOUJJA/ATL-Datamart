########################################## 
###  This file grabs all data tables from the warehouse to the nyc_datamart db
###  then merges all tables into one table "warehouse_data". Once merged they are deleted
##########################################


import psycopg2
from psycopg2 import sql


# Connection details for warehouse and datamart
warehouse_conn = psycopg2.connect(
    dbname="nyc_warehouse",
    user="airflow",
    password="airflow",
    host="localhost",
    port="15433"
)
mart_conn = psycopg2.connect(
    dbname="nyc_datamart",
    user="postgres",
    password="admin",
    host="localhost",
    port="15432"
)


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
        # Unify all warehouse tables into one and then delete the tables
        execute_sql_file(mart_conn, '../../sql/unify_data.sql')
        return 1
    except Exception as e:
        print(f"Problem occured : {e}")
        return 0




def warehouse_to_datamart():
    print('Moving data from warehouse to nyc_datamart!')
    
    try:
        warehouse_cursor = warehouse_conn.cursor()
        mart_cursor = mart_conn.cursor()

        # Step 1: Get all table names from the warehouse
        warehouse_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = warehouse_cursor.fetchall()

        # Step 2: Loop through each table and copy it to the data mart
        for table in tables:
            table_name = table[0]
            print(f"\033[38;5;214mMoving table : {table_name}\033[0m")
            
            # Step 2.1: Get the table structure (columns and their types)
            warehouse_cursor.execute(sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s"), [table_name])
            columns = warehouse_cursor.fetchall()
            
            # Step 2.2: Check if table exists in the data mart
            mart_cursor.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"), [table_name])
            table_exists = mart_cursor.fetchone()[0]

            if not table_exists:
                # Create the table in the data mart if it doesn't exist
                column_definitions = ", ".join([f'"{col[0]}" {col[1]}' for col in columns])
                create_table_sql = f'CREATE TABLE public."{table_name}" ({column_definitions})'
                mart_cursor.execute(create_table_sql)
            
            # Step 2.3: Insert data into the data mart (if the table exists or was created)
            warehouse_cursor.execute(sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(table_name)))
            rows = warehouse_cursor.fetchall()
            
            # Prepare the insert query dynamically for each table
            insert_query = sql.SQL("INSERT INTO public.{} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join([sql.Identifier(col[0]) for col in columns]),
                sql.SQL(", ").join([sql.Placeholder()] * len(columns))
            )
            
            # Insert data in chunks (to avoid memory overflow for large tables)
            chunk_size = 1000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i+chunk_size]
                mart_cursor.executemany(insert_query, chunk)
            
            mart_conn.commit()

        # Close connections
        warehouse_cursor.close()
        mart_cursor.close()
        warehouse_conn.close()
        return 1
    
    except Exception as e:
        print(f"Problem occured : {e}")
        return 0




