########################################## 
###  This file grabs data from the Warehouse to Datamart
##########################################



from psycopg2 import sql
from connection_config import connect_Datamart, connect_Warehouse


def warehouse_to_datamart():
    print("\033[1;32m        ########    Moving Data From Warehouse To Datamart!\033[0m")

    try:
        mart_conn = connect_Datamart()
        warehouse_conn = connect_Warehouse()
        
        warehouse_cursor = warehouse_conn.cursor()
        mart_cursor = mart_conn.cursor()

        # Step 1: Get all table names from the warehouse
        warehouse_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = warehouse_cursor.fetchall()

        # Step 2: Loop through each table and copy it to the data mart
        for table in tables:
            table_name = table[0]            
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
            
            # Step 2.3: Fetch and insert data in chunks
            warehouse_cursor.execute(sql.SQL("SELECT COUNT(*) FROM public.{}").format(sql.Identifier(table_name)))
            total_rows = warehouse_cursor.fetchone()[0]
            chunk_size = 10000  # Number of rows per chunk
            num_chunks = total_rows // chunk_size + (1 if total_rows % chunk_size != 0 else 0)
            
            for i in range(num_chunks):
                offset = i * chunk_size
                warehouse_cursor.execute(
                    sql.SQL("SELECT * FROM public.{} LIMIT %s OFFSET %s").format(sql.Identifier(table_name)),
                    [chunk_size, offset]
                )
                rows = warehouse_cursor.fetchall()
                
                # Prepare the insert query dynamically for each table
                insert_query = sql.SQL("INSERT INTO public.{} ({}) VALUES ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join([sql.Identifier(col[0]) for col in columns]),
                    sql.SQL(", ").join([sql.Placeholder()] * len(columns))
                )
                
                # Insert the chunk into the datamart
                mart_cursor.executemany(insert_query, rows)
                
                # Print progress
                percentage = (i + 1) / num_chunks * 100
                print(f"\r\033[38;5;214mMoving Data From Warehouse To Datamart : {percentage:.2f}%\033[0m", end='')
            
            mart_conn.commit()
            print()  # Move to a new line after finishing each table

        # Close connections
        warehouse_cursor.close()
        mart_cursor.close()
        warehouse_conn.close()
        mart_conn.close()
        return 1
    
    except Exception as e:
        print("\033[1;31m        ########    Problem Occurred While Moving Data To Datamart\033[0m")
        print(e)
        return 0





