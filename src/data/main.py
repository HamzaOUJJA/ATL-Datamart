########################################## 
###  This file automates retrieval process of data from the NewYork State 
###  to the MinIO DataLake and then to the Data WareHouse (Postgres DB)
##########################################

import os
import sys
from grab_Data_From_Source      import grab_Data_From_Source
from grab_Data_From_MinIO       import grab_Data_From_MinIO
from write_Data_To_MinIO        import write_Data_To_MinIO
from write_Data_To_Warehouse    import write_Data_To_Warehouse
from warehouse_to_datamart      import warehouse_to_datamart, unify_data
from create_Marts               import create_Marts, insert_Marts

def main():
    #clean_local_folder()
    #grab_Data_From_Source([2024], list(range(1, 13)), ["green_tripdata", "fhv_tripdata"])       
    #write_Data_To_MinIO()   
    #clean_local_folder()
    #grab_Data_From_MinIO()
    #write_Data_To_Warehouse()
    #warehouse_to_datamart()
    #unify_data()
    #create_Marts()
    insert_Marts()
    pass
    


                        

def clean_local_folder():
    folder_path = '../../data/raw'
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        os.remove(file_path) 
    print("Local Folder Cleaned!")



if __name__ == '__main__':
    sys.exit(main())