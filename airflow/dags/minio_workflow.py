##########################################
###  This file automates retrieval process of data from the NewYork State 
###  to the MinIO DataLake and then to the Data WareHouse (Postgres DB)
##########################################

from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import os
import sys
import subprocess


# Import custom modules
sys.path.insert(1, '../../src/data')
sys.path.insert(1, '../../src/visualization')

from grab_Data_From_Source      import grab_Last_Month, grab_Data_From_Source
from grab_Data_From_MinIO       import grab_Data_From_MinIO
from write_Data_To_MinIO        import write_Data_To_MinIO
from write_Data_To_Warehouse    import write_Data_To_Warehouse
from warehouse_to_datamart      import warehouse_to_datamart
from create_Marts               import create_Marts, insert_Marts
from visualize                  import visualize





def clean_local_folder():
    folder_path = '../../data/raw'
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        os.remove(file_path) 
    print("\033[1;32m        ########    Local Folder Cleaned!\033[0m")


def process_data(years, months):
    #Executes the full data processing pipeline for the given years and months
    clean_local_folder()
    grab_Data_From_Source(years=years, months=months)
    write_Data_To_MinIO()
    grab_Data_From_MinIO()
    write_Data_To_Warehouse()
    create_Marts()
    warehouse_to_datamart()
    insert_Marts()
    print("\033[1;32m        ########    Data Pipeline Completed!\033[0m")


def process_last_month():
    #Executes the data processing pipeline for the last available month
    clean_local_folder()
    grab_Last_Month()
    write_Data_To_MinIO()
    clean_local_folder()
    grab_Data_From_MinIO()
    write_Data_To_Warehouse()
    create_Marts()
    warehouse_to_datamart()
    insert_Marts()
    print("\033[1;32m        ########    Last Month's Data Pipeline Completed!\033[0m")


# Define the DAG for retrieving data for the year 2024
with DAG(
    dag_id="grab_data_for_2024",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",  # Run only once
    catchup=False,
) as dag_2024:

    grab_2024_data_task = PythonOperator(
        task_id="grab_2024_data_task",
        python_callable=process_data,
        op_kwargs={
            "years": [2024],
            "months": list(range(1, 12))  # All months of 2024 except December
        },
    )


# Define the DAG for monthly data retrieval
with DAG(
    dag_id="grab_last_month_data",
    start_date=pendulum.today('UTC').replace(day=1).add(months=-1),  # Start on the first day of the previous month
    schedule="0 0 1 * *",  # Run on the 1st day of every month at midnight
    catchup=False,
) as monthly_dag:

    grab_last_month_data_task = PythonOperator(
        task_id="grab_last_month_data_task",
        python_callable=process_last_month,  # Use the new function to handle last month's data
    )
