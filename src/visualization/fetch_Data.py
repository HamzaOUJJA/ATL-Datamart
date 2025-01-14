from sqlalchemy import create_engine
import pandas as pd
import streamlit as st



# SQLAlchemy Database Connection
def get_db_connection():
    # PostgreSQL connection string
    connection_string = "postgresql+psycopg2://postgres:admin@localhost:15435/nyc_datamart"
    engine = create_engine(connection_string)
    return engine.connect()

# Query function using SQLAlchemy
def fetch_Data(vehicle_type, start_date, end_date):
    query = get_Query(vehicle_type, start_date, end_date)
    try:
        conn = get_db_connection()  # Using SQLAlchemy engine connection
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()




def get_Query(vehicle_type, start_date, end_date):

    # Build query with filters
    where_clauses = []
    if vehicle_type != "All":
        where_clauses.append(f"vehicul_type = '{vehicle_type}'")
    if start_date:
        where_clauses.append(f"CAST(dim_datetime.pickup_datetime AS DATE) >= '{start_date.strftime('%Y-%m-%d')}'")
    if end_date:
        where_clauses.append(f"CAST(dim_datetime.pickup_datetime AS DATE) <= '{end_date.strftime('%Y-%m-%d')}'")
    
    where_clause = " AND ".join(where_clauses)
    where_clause = f"WHERE {where_clause}" if where_clauses else ""

    if (vehicle_type == 'All'):
        query = f"""
            SELECT 
                fact_trip."trip_id", 
                fact_trip."vehicul_type", 

                dim_location."PULocationID", 
                dim_location."DOLocationID",

                CAST(dim_datetime."pickup_datetime" AS TIMESTAMP) AS pickup_datetime, 
                CAST(dim_datetime."dropoff_datetime" AS TIMESTAMP) AS dropoff_datetime, 
                dim_datetime."request_datetime", 
                dim_datetime."on_scene_datetime", 

                CAST(dim_trip_details."trip_distance" AS FLOAT) AS trip_distance,
                dim_trip_details."passenger_count", 
                dim_trip_details."trip_type",
                dim_trip_details."VendorID", 
                dim_trip_details."RatecodeID",
                dim_trip_details."trip_time",

                CAST(dim_fare."total_fare" AS FLOAT) AS total_fare,
                CAST(dim_fare."fare" AS FLOAT) AS fare,
                CAST(dim_fare."tolls_amount" AS FLOAT) AS tolls_amount,
                CAST(dim_fare."tip_amount" AS FLOAT) AS tip_amount,
                dim_fare."improvement_surcharge",
                dim_fare."extra",
                dim_fare."mta_tax",
                dim_fare."congestion_surcharge",
                dim_fare."airport_fee",
                dim_fare."ehail_fee",
                dim_fare."bcf",
                dim_fare."payment_type",

                dim_flags."shared_request_flag", 
                dim_flags."shared_match_flag", 
                dim_flags."wav_request_flag", 
                dim_flags."wav_match_flag",
                dim_flags."access_a_ride_flag",
                dim_flags."store_and_fwd_flag",
                dim_flags."SR_Flag",

                dim_base."hvfhs_license_num", 
                dim_base."originating_base_num", 
                dim_base."dispatching_base_num", 
                dim_base."Affiliated_base_number" 

            FROM fact_trip
            JOIN dim_datetime ON fact_trip.datetime_id = dim_datetime.datetime_id
            JOIN dim_trip_details ON fact_trip.details_id = dim_trip_details.details_id
            JOIN dim_fare ON fact_trip.fare_id = dim_fare.fare_id
            JOIN dim_flags ON fact_trip.flags_id = dim_flags.flags_id
            JOIN dim_location ON fact_trip.location_id = dim_location.location_id
            JOIN dim_base ON fact_trip.base_id = dim_base.base_id
            {where_clause}
        """

    elif (vehicle_type == 'green'):
        query = f"""
            SELECT 
                fact_trip."trip_id", 
                fact_trip."vehicul_type", 

                dim_location."PULocationID", 
                dim_location."DOLocationID",

                CAST(dim_datetime."pickup_datetime" AS TIMESTAMP) AS pickup_datetime, 
                CAST(dim_datetime."dropoff_datetime" AS TIMESTAMP) AS dropoff_datetime, 

                CAST(dim_trip_details."trip_distance" AS FLOAT) AS trip_distance,
                dim_trip_details."passenger_count", 
                dim_trip_details."trip_type",
                dim_trip_details."VendorID", 
                dim_trip_details."RatecodeID",

                CAST(dim_fare."total_fare" AS FLOAT) AS total_fare,
                CAST(dim_fare."fare" AS FLOAT) AS fare,
                CAST(dim_fare."tolls_amount" AS FLOAT) AS tolls_amount,
                CAST(dim_fare."tip_amount" AS FLOAT) AS tip_amount,
                dim_fare."improvement_surcharge",
                dim_fare."extra",
                dim_fare."mta_tax",
                dim_fare."congestion_surcharge",
                dim_fare."ehail_fee",
                dim_fare."payment_type",

                dim_flags."store_and_fwd_flag"

            FROM fact_trip
            JOIN dim_datetime ON fact_trip.datetime_id = dim_datetime.datetime_id
            JOIN dim_trip_details ON fact_trip.details_id = dim_trip_details.details_id
            JOIN dim_fare ON fact_trip.fare_id = dim_fare.fare_id
            JOIN dim_flags ON fact_trip.flags_id = dim_flags.flags_id
            JOIN dim_location ON fact_trip.location_id = dim_location.location_id
            JOIN dim_base ON fact_trip.base_id = dim_base.base_id
            {where_clause}
        """

    elif (vehicle_type == 'yellow'):
        query = f"""
            SELECT 
                fact_trip."trip_id", 
                fact_trip."vehicul_type", 

                dim_location."PULocationID", 
                dim_location."DOLocationID",

                CAST(dim_datetime."pickup_datetime" AS TIMESTAMP) AS pickup_datetime, 
                CAST(dim_datetime."dropoff_datetime" AS TIMESTAMP) AS dropoff_datetime, 

                CAST(dim_trip_details."trip_distance" AS FLOAT) AS trip_distance,
                dim_trip_details."passenger_count", 
                dim_trip_details."VendorID", 
                dim_trip_details."RatecodeID",

                CAST(dim_fare."total_fare" AS FLOAT) AS total_fare,
                CAST(dim_fare."fare" AS FLOAT) AS fare,
                CAST(dim_fare."tolls_amount" AS FLOAT) AS tolls_amount,
                CAST(dim_fare."tip_amount" AS FLOAT) AS tip_amount,
                dim_fare."improvement_surcharge",
                dim_fare."extra",
                dim_fare."mta_tax",
                dim_fare."congestion_surcharge",
                dim_fare."airport_fee",
                dim_fare."payment_type",

                dim_flags."store_and_fwd_flag"

            FROM fact_trip
            JOIN dim_datetime ON fact_trip.datetime_id = dim_datetime.datetime_id
            JOIN dim_trip_details ON fact_trip.details_id = dim_trip_details.details_id
            JOIN dim_fare ON fact_trip.fare_id = dim_fare.fare_id
            JOIN dim_flags ON fact_trip.flags_id = dim_flags.flags_id
            JOIN dim_location ON fact_trip.location_id = dim_location.location_id
            JOIN dim_base ON fact_trip.base_id = dim_base.base_id
            {where_clause}
        """
    
    elif (vehicle_type == 'fhvhv'):
        query = f"""
            SELECT 
                fact_trip."trip_id", 
                fact_trip."vehicul_type", 

                dim_location."PULocationID", 
                dim_location."DOLocationID",

                CAST(dim_datetime."pickup_datetime" AS TIMESTAMP) AS pickup_datetime, 
                CAST(dim_datetime."dropoff_datetime" AS TIMESTAMP) AS dropoff_datetime, 
                dim_datetime."request_datetime", 
                dim_datetime."on_scene_datetime", 

                CAST(dim_trip_details."trip_distance" AS FLOAT) AS trip_distance,
                dim_trip_details."trip_time",

                CAST(dim_fare."total_fare" AS FLOAT) AS total_fare,
                CAST(dim_fare."fare" AS FLOAT) AS fare,
                CAST(dim_fare."tolls_amount" AS FLOAT) AS tolls_amount,
                CAST(dim_fare."tip_amount" AS FLOAT) AS tip_amount,
                dim_fare."mta_tax",
                dim_fare."congestion_surcharge",
                dim_fare."airport_fee",
                dim_fare."bcf",

                dim_flags."shared_request_flag", 
                dim_flags."shared_match_flag", 
                dim_flags."wav_request_flag", 
                dim_flags."wav_match_flag",
                dim_flags."access_a_ride_flag",

                dim_base."hvfhs_license_num", 
                dim_base."originating_base_num", 
                dim_base."dispatching_base_num"

            FROM fact_trip
            JOIN dim_datetime ON fact_trip.datetime_id = dim_datetime.datetime_id
            JOIN dim_trip_details ON fact_trip.details_id = dim_trip_details.details_id
            JOIN dim_fare ON fact_trip.fare_id = dim_fare.fare_id
            JOIN dim_flags ON fact_trip.flags_id = dim_flags.flags_id
            JOIN dim_location ON fact_trip.location_id = dim_location.location_id
            JOIN dim_base ON fact_trip.base_id = dim_base.base_id
            {where_clause}
        """
    
    elif (vehicle_type == 'fhv'):
        query = f"""
            SELECT 
                fact_trip."trip_id", 
                fact_trip."vehicul_type", 

                dim_location."PULocationID", 
                dim_location."DOLocationID",

                CAST(dim_datetime."pickup_datetime" AS TIMESTAMP) AS pickup_datetime, 
                CAST(dim_datetime."dropoff_datetime" AS TIMESTAMP) AS dropoff_datetime, 
                
                dim_base."dispatching_base_num", 
                dim_base."Affiliated_base_number",

                dim_flags."SR_Flag"

            FROM fact_trip
            JOIN dim_datetime ON fact_trip.datetime_id = dim_datetime.datetime_id
            JOIN dim_trip_details ON fact_trip.details_id = dim_trip_details.details_id
            JOIN dim_fare ON fact_trip.fare_id = dim_fare.fare_id
            JOIN dim_flags ON fact_trip.flags_id = dim_flags.flags_id
            JOIN dim_location ON fact_trip.location_id = dim_location.location_id
            JOIN dim_base ON fact_trip.base_id = dim_base.base_id
            {where_clause}
        """
    return query