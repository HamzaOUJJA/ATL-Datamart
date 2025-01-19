########################################## 
###  This file uses StreamLit to create a data visualisation dashboard
##########################################

import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import date
from fetch_Data import fetch_Data

# Function to load external CSS file
def load_css():
    try:
        with open("../../src/visualization/style.css", "r") as f:
            css = f.read()
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.warning("CSS file not found.")



# Main dashboard function (remains unchanged)
def visualize():
    load_css()

    # Filter Inputs
    vehicle_type = st.selectbox("Vehicle Type", ['All', 'yellow', 'green', 'fhv', 'fhvhv'])
    start_date = st.date_input("Start Date", value=date(2024, 10, 1))
    end_date   = st.date_input("End Date",   value=date(2024, 11, 1))


    data = fetch_Data(vehicle_type, start_date, end_date)
    data = Convert_Data(data)
    


    if data.empty:
        st.warning("No data available for the selected filters.")
    else:
        

        General_Metrics(data, vehicle_type)
        Data_Preview(data, vehicle_type)
        





        

        
def General_Metrics(data, vehicle_type):
    if (vehicle_type == 'All'):
        st.write("### General Metrics")
        st.metric("Total Trips", len(data))
        st.metric("Average Fare", round(data["total_fare"].mean(), 2))
        st.metric("Average Trip Distance", round(data["trip_distance"].mean(), 2))




def Data_Preview(data, vehicle_type):
    with st.expander("### Data Preview"):
        # Add the column selection widget
        all_columns = list(data.columns)  # Get all column names
        selected_columns = st.multiselect(
            "Select Columns to Display",
            options=all_columns,
            default=all_columns,  # Default to show all columns
            help="Reorder columns by dragging them and deselect to hide."
        )

        # Rearrange data based on selection
        if selected_columns:  # Ensure at least one column is selected
            data = data[selected_columns]
        else:
            st.warning("Please select at least one column to display.")

        st.dataframe(data)







    


def Convert_Data(data):
    data['PULocationID'] = data['PULocationID'].astype(str).apply(lambda x: x.split('.')[0])
    data['DOLocationID'] = data['DOLocationID'].astype(str).apply(lambda x: x.split('.')[0])
    location_data = pd.read_csv('../../data/taxi_zone_lookup.csv')
    location_data['LocationID'] = location_data['LocationID'].astype(str)
    location_mapping = dict(zip(location_data['LocationID'], location_data['Zone']))
    data['PULocationName'] = data['PULocationID'].map(location_mapping)
    data['DOLocationName'] = data['DOLocationID'].map(location_mapping)
    data = data.drop(columns=['PULocationID'])
    data = data.drop(columns=['DOLocationID'])

    # Convert date columns to datetime
    data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"], errors="coerce")
    data["dropoff_datetime"] = pd.to_datetime(data["dropoff_datetime"], errors="coerce")
    return data









if __name__ == "__main__":
    visualize()