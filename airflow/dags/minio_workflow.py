########################################## 
###  This file automates retrieval process of data from the NewYork State 
###  to the MinIO DataLake and then to the Data WareHouse (Postgres DB)
##########################################

#from minio import Minio
#from airflow import DAG
#from airflow.operators.python import PythonOperator
#import pendulum
import os
import requests
import sys


sys.path.insert(1, '../../src/data')
sys.path.insert(1, '../../src/visualization')


from grab_Data_From_Source      import grab_Data_From_Source
from grab_Data_From_MinIO       import grab_Data_From_MinIO
from write_Data_To_MinIO        import write_Data_To_MinIO
from write_Data_To_Warehouse    import write_Data_To_Warehouse
from warehouse_to_datamart      import warehouse_to_datamart
from unify_data                 import unify_data
from create_Marts               import create_Marts, insert_Marts
from visualize                  import visualize





def main():
    try:
        #clean_local_folder()
        #grab_Data_From_Source([2024], [10])
        #write_Data_To_MinIO()   
        #clean_local_folder()
        #grab_Data_From_MinIO()
        #write_Data_To_Warehouse()
        #warehouse_to_datamart()
        #unify_data()
        #create_Marts()
        #insert_Marts()
        pass

    except Exception as e:
        print("\033[1;31m ###### Exception Occured In The Main ######\033[0m")
        print(e)
    pass  



                        

def clean_local_folder():
    folder_path = '../../data/raw'
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        os.remove(file_path) 
    print("Local Folder Cleaned!")



if __name__ == '__main__':
    sys.exit(main())




# # Define the DAG
# with DAG(
#     dag_id="grab_data_dag",
#     start_date=pendulum.today('UTC').add(days=-1),
#     schedule=None,
#     catchup=False,
# ) as dag:

#     # Task: Grab data from source with custom parameters
#     grab_data_task = PythonOperator(
#         task_id="grab_data_task",
#         python_callable=grab_Data_From_Source,
#         op_kwargs={
#             "years": [2024],         
#             "months": [10]
#         },
#     )
