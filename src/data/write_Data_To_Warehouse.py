########################################## 
###  This file uploads data in the local folder ../../data/raw
###  The data is then uploaded to the PostgresDB
###  Each parquet file is uploaded to a table in the DB
##########################################

import os
import pandas as pd
from sqlalchemy import create_engine



def write_Data_To_Warehouse():
    print('Uploading Data To PostgresDB!')
    directory = '../../data/raw'
    db_connection_string = 'postgresql+psycopg2://postgres:admin@localhost:15432/nyc_warehouse'

    try:
        engine = create_engine(db_connection_string)
        
        for filename in os.listdir(directory):
            if filename.endswith('.parquet'):
                print(f"\033[38;5;214mUploading {filename} to PostgresDB\033[0m")
                parquet_file = os.path.join(directory, filename)
                table_name = os.path.splitext(filename)[0]
                df = pd.read_parquet(parquet_file)
                
                # Convert all columns to type text
                df = df.astype(str)

                df.to_sql(table_name, engine, if_exists='replace', index=False)  # Pass engine directly
        return 1
    except Exception as e:
        print("\033[1;31m ###### Problem occured while uploading data to warehouse! ######\033[0m")
        print(e)
        return 0
