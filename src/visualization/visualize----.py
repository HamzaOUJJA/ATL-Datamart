########################################## 
###  This file uses StreamLit to create a data visualisation dashboard
##########################################

from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

# Function to load external CSS file
def load_css():
    try:
        with open("style.css", "r") as f:
            css = f.read()
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning("CSS file not found.")



# SQLAlchemy Database Connection
def get_db_connection():
    # PostgreSQL connection string
    connection_string = "postgresql+psycopg2://postgres:admin@localhost:15435/nyc_datamart"
    engine = create_engine(connection_string)
    return engine.connect()

# Query function using SQLAlchemy
def fetch_data(query):
    try:
        conn = get_db_connection()  # Using SQLAlchemy engine connection
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# Main dashboard function (remains unchanged)
def visualize():
    # Filter Inputs
    vehicle_types = fetch_data("SELECT DISTINCT vehicul_type FROM fact_trip")
    vehicle_type = st.sidebar.selectbox("Vehicle Type", vehicle_types["vehicul_type"].tolist() if not vehicle_types.empty else ["All"])
    start_date = st.sidebar.date_input("Start Date")
    end_date = st.sidebar.date_input("End Date")

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

    # Modified query with proper casting for TEXT fields
    query = f"""
        SELECT 
            fact_trip.trip_id, 
            fact_trip.vehicul_type, 
            CAST(dim_datetime.pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
            CAST(dim_datetime.dropoff_datetime AS TIMESTAMP) AS dropoff_datetime, 
            CAST(dim_trip_details.trip_distance AS FLOAT) AS trip_distance,
            CAST(dim_fare.total_fare AS FLOAT) AS total_fare,
            CAST(dim_fare.fare AS FLOAT) AS fare,
            CAST(dim_fare.tolls_amount AS FLOAT) AS tolls_amount,
            CAST(dim_fare.tip_amount AS FLOAT) AS tip_amount,
            dim_trip_details.passenger_count, 
            dim_trip_details.trip_type,
            dim_flags.shared_request_flag, 
            dim_flags.shared_match_flag, 
            dim_flags.wav_request_flag, 
            dim_flags.wav_match_flag,
            dim_location."PULocationID", 
            dim_location."DOLocationID", 
            dim_trip_details."VendorID", 
            dim_trip_details."RatecodeID"
        FROM fact_trip
        JOIN dim_datetime ON fact_trip.datetime_id = dim_datetime.datetime_id
        JOIN dim_trip_details ON fact_trip.details_id = dim_trip_details.details_id
        JOIN dim_fare ON fact_trip.fare_id = dim_fare.fare_id
        JOIN dim_flags ON fact_trip.flags_id = dim_flags.flags_id
        JOIN dim_location ON fact_trip.location_id = dim_location.location_id
        {where_clause}
    """
    data = fetch_data(query)
    print(data)

    if data.empty:
        st.warning("No data available for the selected filters.")
    else:
        # Convert date columns to datetime
        data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"], errors="coerce")
        data["dropoff_datetime"] = pd.to_datetime(data["dropoff_datetime"], errors="coerce")

        # General Metrics
        st.write("### Data Preview")
        st.dataframe(data)

        st.write("### General Metrics")
        st.metric("Total Trips", len(data))
        st.metric("Average Fare", round(data["total_fare"].mean(), 2))
        st.metric("Average Trip Distance", round(data["trip_distance"].mean(), 2))

        # Visualization 1: Trips Over Time
        st.write("### Trips Over Time")
        trips_over_time = data.groupby(data["pickup_datetime"].dt.date).size().reset_index(name="Number of Trips")
        fig_trips_time = px.line(trips_over_time, x="pickup_datetime", y="Number of Trips", title="Trips Over Time")
        st.plotly_chart(fig_trips_time)

        # Visualization 2: Trip Distance Distribution
        st.write("### Trip Distance Distribution")
        fig_distance = px.histogram(data, x="trip_distance", nbins=30, title="Trip Distance Distribution")
        st.plotly_chart(fig_distance)

        # Visualization 3: Fare Breakdown
        st.write("### Fare Breakdown")
        fare_components = ["fare", "tolls_amount", "tip_amount"]
        fig_fare = px.bar(data[fare_components].mean().reset_index(name="Average Amount"), 
                          x="index", y="Average Amount", title="Average Fare Components")
        st.plotly_chart(fig_fare)

        # Visualization 4: Trip Type Distribution
        st.write("### Trip Type Distribution")
        fig_trip_type = px.pie(data, names="trip_type", title="Trip Type Distribution")
        st.plotly_chart(fig_trip_type)

        # Visualization 5: Flags Distribution
        st.write("### Flags Distribution")
        flag_columns = ["shared_request_flag", "shared_match_flag", "wav_request_flag", "wav_match_flag"]
        flags_data = data[flag_columns].melt(var_name="Flag", value_name="Value")
        fig_flags = px.histogram(flags_data, x="Flag", color="Value", title="Flags Distribution")
        st.plotly_chart(fig_flags)

        # Visualization 6: Popular Pickup/Drop-off Locations
        st.write("### Popular Pickup/Drop-off Locations")
        locations_data = data.groupby(["PULocationID", "DOLocationID"]).size().reset_index(name="Number of Trips")
        fig_locations = px.scatter(locations_data, x="PULocationID", y="DOLocationID", size="Number of Trips",
                                    title="Popular Pickup vs Drop-off Locations")
        st.plotly_chart(fig_locations)

        # Visualization 7: Vendor Metrics
        st.write("### Vendor Metrics")
        fig_vendor = px.histogram(data, x="VendorID", title="Trips by Vendor")
        st.plotly_chart(fig_vendor)




if __name__ == "__main__":
    visualize()