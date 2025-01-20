########################################## 
###  This file uses StreamLit to create a data visualisation dashboard
##########################################

import pandas as pd
import streamlit as st
import plotly.express as px
from   datetime import date
from   fetch_Data import fetch_Data


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

    #vehicle_type = st.selectbox("Vehicle Type", ['All', 'yellow', 'green', 'fhv', 'fhvhv'])
    vehicle_type = 'All'
    col_1_1, col_1_2, col_1_3 = st.columns([1, 4, 1])
    start_date = col_1_1.date_input("Start Date", value=date(2024, 10, 1))
    col_1_2.title("NYC Data Dashboard")    
    end_date   = col_1_3.date_input("End Date",   value=date(2024, 11, 1))

    data = fetch_Data(vehicle_type, start_date, end_date)
    data = convert_Data(data)
    
    if data.empty:
        st.warning("No data available for the selected filters.")
    else:
        data_Preview(data)

        col_2_1, col_2_2, col_2_3, col_2_4 = st.columns([1, 1, 1, 1])
        col_2_1.metric("Total Revenue ($)", round(data["total_fare"].sum()))
        col_2_2.metric("Total Trips", len(data))
        col_2_3.metric("Average Fare ($)", round(data["total_fare"].mean(), 2))
        col_2_4.metric("Average Trip Distance (miles)", round(data["trip_distance"].mean(), 2))
        



        # Visualization 1: Trip Location Distribution
        col_3_1, col_3_2, col_3_3, col_3_4, col_3_5 = st.columns(5)
        
        fig_vehicul_type    = px.pie(data, names="vehicul_type", title="Vehicul Type Distribution")
        fig_vehicul_type.update_traces( textinfo="label+percent+value", textposition="inside",pull=None)
        col_3_1.plotly_chart(fig_vehicul_type)

        # Visualization 2: Trip Distance Distribution
        fig_distance = px.histogram(data, x="trip_distance", nbins=30, title="Trip Distance Distribution")
        col_3_2.plotly_chart(fig_distance)

        # Convert the pie chart to a histogram-style bar chart
        fig_trip_type = px.pie(data, names="trip_type", title="Trip Type Distribution",labels={"trip_type": "Trip Type"},color="trip_type")
        col_3_3.plotly_chart(fig_trip_type)

        fig_vendor = px.pie(data, names="VendorID", title="Trips by Vendor",color="VendorID")
        col_3_4.plotly_chart(fig_vendor)

        # Visualization 3: Fare Breakdown as a pie chart
        fare_components = ["fare", "tolls_amount", "tip_amount"]
        fare_data = data[fare_components].mean().reset_index(name="Average Amount")
        fig_fare_pie = px.pie(fare_data,names="index",  values="Average Amount",  title="Average Fare Components",)
        col_3_5.plotly_chart(fig_fare_pie)



        col_4_1, col_4_2 = st.columns([1, 3])
        # Visualization 5: Flags Distribution (Group Bar Chart)
        flag_columns = ["shared_request_flag", "shared_match_flag", "wav_request_flag", "wav_match_flag"]

        # Préparer les données pour la visualisation
        flags_data = data[flag_columns].melt(var_name="Flag", value_name="Value")

        # Créer un graphique de barres groupées
        fig_flags_grouped = px.bar(
            flags_data,
            x="Flag",         # Les catégories (noms des flags)
            color="Value",    # Les couleurs basées sur les valeurs (0 ou 1)
            barmode="group",  # Options : "group" pour barres groupées, "stack" pour barres empilées
            title="Flags Distribution (Grouped Bar Chart)",
            labels={"Value": "Flag Value", "Flag": "Flag Type"},
        )

        # Afficher le graphique dans la colonne
        col_4_1.plotly_chart(fig_flags_grouped)
        


        




        # Location Histograms
        data1 = data.dropna(subset=["PULocationName", "DOLocationName"])
        fig_DOLocation = px.histogram(data1, x="DOLocationName", title="Drop Off Location Distribution",labels={"DOLocationName": "Dropoff Location"},color="DOLocationName")
        fig_DOLocation.update_layout(showlegend=False)
        col_4_2.plotly_chart(fig_DOLocation)
        fig_PULocation = px.histogram(data1, x="PULocationName", title="Pick Up Location Distribution",labels={"PULocationName": "Pickup Location"},color="PULocationName")
        fig_PULocation.update_layout(showlegend=False)
        col_4_2.plotly_chart(fig_PULocation)






        
        


        
    
        
        





        

        




def data_Preview(data):
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



def convert_Data(data):
    data['PULocationID'] = data['PULocationID'].astype(str).apply(lambda x: x.split('.')[0])
    data['DOLocationID'] = data['DOLocationID'].astype(str).apply(lambda x: x.split('.')[0])
    location_data = pd.read_csv('../../data/taxi_zone_lookup.csv')
    location_data['LocationID'] = location_data['LocationID'].astype(str)
    location_mapping = dict(zip(location_data['LocationID'], location_data['Zone']))
    data['PULocationName'] = data['PULocationID'].map(location_mapping)
    data['DOLocationName'] = data['DOLocationID'].map(location_mapping)
    data = data.drop(columns=['PULocationID'])
    data = data.drop(columns=['DOLocationID'])

    data["trip_type"] = data["trip_type"].replace({"1": "Street-hail", "2": "Dispatch"})
    data["VendorID"] = data["VendorID"].replace({"1": "CMT, LLC", "2": "VeriFone Inc."})

    # Convert date columns to datetime
    data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"], errors="coerce")
    data["dropoff_datetime"] = pd.to_datetime(data["dropoff_datetime"], errors="coerce")
    return data









if __name__ == "__main__":
    visualize()