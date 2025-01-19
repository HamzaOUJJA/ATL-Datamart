########################################## 
###  This file creates a table in the warehouse with the name warehouse_data
###  After that it uploads each data file in the local folder '../../data/row' to it
##########################################



import os
import pandas as pd
from sqlalchemy import create_engine
from connection_config import connect_Warehouse


def write_Data_To_Warehouse():
    print("\033[1;32m        ########    Uploading Data To WareHouse!\033[0m")

    directory = '../../data/raw'
    db_connection_string = 'postgresql+psycopg2://postgres:admin@localhost:15432/nyc_warehouse'

    try:
        engine = create_engine(db_connection_string)
        create_warehouse_data_table()
        
        for filename in os.listdir(directory):
            if filename.endswith('.parquet'):
                parquet_file = os.path.join(directory, filename)
                table_name = os.path.splitext(filename)[0]
                
                # Load the entire Parquet file
                df = pd.read_parquet(parquet_file)
                
                # Process in chunks
                chunk_size = 10000  # Number of rows per chunk
                num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)

                # Initialize progress
                for i in range(num_chunks):
                    start = i * chunk_size
                    end = (i + 1) * chunk_size
                    chunk = df.iloc[start:end]
                    
                    # Calculate the progress percentage
                    percentage = (i + 1) / num_chunks * 100
                    # Print the progress on the same line
                    print(f"\r\033[38;5;214mUploading {filename}: {percentage:.3f}%\033[0m", end='')

                    # Convert all columns to string for compatibility
                    # chunk = chunk.astype(str)

                    # Append the chunk to the database
                    chunk.to_sql(table_name, engine, if_exists='append', index=False)
                
                # Move to the next line after the final progress print
                print()
                insert_table_to_warehouse(filename)
        return 1
                
    except Exception as e:
        print("\033[1;31m        ########    Problem Occurred While Uploading Data To Warehouse!\033[0m")
        print(e)
        return 0







def execute_sql(connection, sql_query):
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        connection.commit()
        cursor.close()
        connection.close()





def insert_table_to_warehouse(filename):
    prefix = filename.split('_')[0].lower()  # Get the part before the first underscore
    base_filename = filename.replace('.parquet', '')  # Remove the .parquet extension

    # Define the SQL query based on the filename prefix
    if prefix == "fhv":
        sql_query = f"""
        -- FHV Tripdata
        INSERT INTO warehouse_data (
            "vehicul_type", "dispatching_base_num","pickup_datetime", "dropoff_datetime", 
            "PULocationID", "DOLocationID", "SR_Flag","Affiliated_base_number")
        SELECT 
            'fhv', "dispatching_base_num","pickup_datetime", "dropOff_datetime", 
            "PUlocationID", "DOlocationID", "SR_Flag","Affiliated_base_number"
        FROM "{base_filename}";
        DROP TABLE IF EXISTS "{base_filename}"; 
        """

    elif prefix == "fhvhv":
        sql_query = f"""
        -- FHVHV Tripdata
        INSERT INTO warehouse_data (
            "vehicul_type", "hvfhs_license_num", "dispatching_base_num", "originating_base_num", "request_datetime",
            "on_scene_datetime", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID", "trip_distance", 
            "trip_time", "fare", "tolls_amount", "bcf","mta_tax", "congestion_surcharge", "airport_fee", "tip_amount",
            "total_fare", "shared_request_flag", "shared_match_flag", "access_a_ride_flag","wav_request_flag","wav_match_flag")
        SELECT 
            'fhvhv', "hvfhs_license_num","dispatching_base_num","originating_base_num","request_datetime", "on_scene_datetime",
            "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID", "trip_miles", "trip_time",
            "base_passenger_fare", "tolls", "bcf","sales_tax","congestion_surcharge", "airport_fee", "tips", "driver_pay",
            "shared_request_flag","shared_match_flag","access_a_ride_flag","wav_request_flag","wav_match_flag"
        FROM "{base_filename}";
        DROP TABLE IF EXISTS "{base_filename}"; 
        """

    elif prefix == "green":
        sql_query = f"""
        -- Green Tripdata 
        INSERT INTO warehouse_data (
            "vehicul_type", "VendorID", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID", 
            "passenger_count", "trip_distance", "fare", "extra", "mta_tax", "tip_amount", "tolls_amount", 
            "ehail_fee", "improvement_surcharge", "total_fare", "payment_type", "trip_type", "congestion_surcharge")
        SELECT 
            'green', "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "RatecodeID", 
            "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", 
            "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type", "trip_type", "congestion_surcharge"
        FROM "{base_filename}";
        DROP TABLE IF EXISTS "{base_filename}"; 
        """

    elif prefix == "yellow":
        sql_query = f"""
        -- Yellow Tripdata
        INSERT INTO warehouse_data (
            "vehicul_type", "VendorID", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance","RatecodeID",
            "store_and_fwd_flag","PULocationID", "DOLocationID", "payment_type","fare", "extra", "mta_tax", "tip_amount", 
            "tolls_amount", "improvement_surcharge","total_fare","congestion_surcharge", "airport_fee")
        SELECT 
            'yellow',"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", 
            "RatecodeID","store_and_fwd_flag","PULocationID", "DOLocationID","payment_type", "fare_amount", "extra", "mta_tax", 
            "tip_amount", "tolls_amount", "improvement_surcharge","total_amount","congestion_surcharge","Airport_fee"
        FROM "{base_filename}";
        DROP TABLE IF EXISTS "{base_filename}"; 
        """
    warehouse_conn = connect_Warehouse()
    print(f"\033[38;5;214mInserting Table : {filename}\033[0m")
    execute_sql(warehouse_conn, sql_query)




def create_warehouse_data_table(): 
    warehouse_conn = connect_Warehouse()
    execute_sql(warehouse_conn, 
        """
        CREATE TABLE IF NOT EXISTS warehouse_data (
            "trip_id"               SERIAL PRIMARY KEY,
            "vehicul_type"          TEXT,                   -- green    yellow  fhv fhvhv
            "pickup_datetime"       TEXT,                   -- green    yellow  fhv fhvhv
            "dropoff_datetime"      TEXT,                   -- green    yellow  fhv fhvhv
            "PULocationID"          TEXT,                   -- green    yellow  fhv fhvhv
            "DOLocationID"          TEXT,                   -- green    yellow  fhv fhvhv

            "trip_type"             TEXT,                   -- green
            "ehail_fee"	            TEXT,                   -- green
            "VendorID"              TEXT,                   -- green    yellow
            "passenger_count"       TEXT,                   -- green    yellow
            "trip_distance"         TEXT,                   -- green    yellow      fhvhv
            "extra"                 TEXT,                   -- green    yellow
            "store_and_fwd_flag"	TEXT,                   -- green    yellow
            "RatecodeID"	        TEXT,                   -- green    yellow
            "improvement_surcharge" TEXT,                   -- green    yellow
            "payment_type"          TEXT,                   -- green    yellow
            "total_fare"            TEXT,                   -- green    yellow      fhvhv
            "fare"                  TEXT,                   -- green    yellow      fhvhv
            "tolls_amount"          TEXT,                   -- green    yellow      fhvhv
            "mta_tax"               TEXT,                   -- green    yellow      fhvhv
            "tip_amount"            TEXT,                   -- green    yellow      fhvhv
            "congestion_surcharge"  TEXT,                   -- green    yellow      fhvhv
            "airport_fee"           TEXT,                   --          yellow      fhvhv
            "hvfhs_license_num"     TEXT,                   --                      fhvhv
            "originating_base_num"  TEXT,                   --                      fhvhv
            "request_datetime"      TEXT,                   --                      fhvhv
            "on_scene_datetime"     TEXT,                   --                      fhvhv
            "trip_time"             TEXT,                   --                      fhvhv
            "bcf"                   TEXT,                   --                      fhvhv
            "shared_request_flag"   TEXT,                   --                      fhvhv
            "shared_match_flag"     TEXT,                   --                      fhvhv
            "access_a_ride_flag"	TEXT,                   --                      fhvhv
            "wav_request_flag"	    TEXT,                   --                      fhvhv
            "wav_match_flag"        TEXT,                   --                      fhvhv
            "dispatching_base_num"  TEXT,                   --                  fhv fhvhv
            "SR_Flag"               TEXT,                   --                  fhv
            "Affiliated_base_number"TEXT                    --                  fhv
        )
        """
        )

    





































# import os
# import pandas as pd
# from sqlalchemy import create_engine



# def write_Data_To_Warehouse():
#     print('Uploading Data To PostgresDB!')
#     directory = '../../data/raw'
#     db_connection_string = 'postgresql+psycopg2://postgres:admin@localhost:15432/nyc_warehouse'

#     try:
#         engine = create_engine(db_connection_string)
        
#         for filename in os.listdir(directory):
#             if filename.endswith('.parquet'):
#                 print(f"\033[38;5;214mUploading {filename} to PostgresDB\033[0m")
#                 parquet_file = os.path.join(directory, filename)
#                 table_name = os.path.splitext(filename)[0]
#                 df = pd.read_parquet(parquet_file)
                
#                 # Convert all columns to type text
#                 df = df.astype(str)

#                 df.to_sql(table_name, engine, if_exists='replace', index=False)  # Pass engine directly
#         return 1
#     except Exception as e:
#         print("\033[1;31m ###### Problem occured while uploading data to warehouse! ######\033[0m")
#         print(e)
#         return 0
