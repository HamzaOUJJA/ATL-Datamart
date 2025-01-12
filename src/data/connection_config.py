########################################## 
###  This file is used to extablish the connection with 
###  the two DBMS warehouse and datamart
##########################################


import psycopg2
import streamlit as st
from sqlalchemy import create_engine




def connect_Warehouse():
    try:
        warehouse_conn = psycopg2.connect(
            dbname="nyc_warehouse",
            user="postgres",
            password="admin",
            host="localhost",
            port="15432"
        )
        return warehouse_conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        print("\033[1;31m ###### Error Connecting to Warehouse! ######\033[0m")
        print(e)
        return None
    

def connect_Datamart():
    try:
        mart_conn = psycopg2.connect(
            dbname="nyc_datamart",
            user="postgres",
            password="admin",
            host="localhost",
            port="15435"
        )
        return mart_conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        print("\033[1;31m ###### Error Connecting to Datamart! ######\033[0m")
        print(e)
        return None


