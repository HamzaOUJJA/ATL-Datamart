########################################## 
###  This file grabs data from NewYork State at https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
###  then stores it in the local folder ../../data/raw
###  To grab last year's data use  : grab_Data_From_Source([2024], list(range(1, 13)))
##########################################


import requests
import os
import datetime



def grab_Data_From_Source(years: list = [2024], months: list = [10], trip_types: list = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]):
    print("\033[1;32m        ########    Downloading Data From The Source!\033[0m")

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    trip_types = trip_types
    files_to_download = [
        f"{trip_type}_{year}-{month:02}.parquet"
        for trip_type in trip_types
        for year in years
        for month in months
    ]

    base_dir = os.path.join("..", "..", "data", "raw")
    
    try:
        for file_name in files_to_download:
            url = f"{base_url}{file_name}"
            file_path = os.path.join(base_dir, file_name)
            
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))  # Total size in bytes
                downloaded_size = 0
                
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=256*1024):  # Download in 1KB chunks
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        percentage = (downloaded_size / total_size) * 100
                        print(f"\r\033[38;5;214mDownloading From Internet : {file_name} : {percentage:.2f}%\033[0m", end="")
                print() 
            else:
                print(f"Failed to download {file_name}. Status code: {response.status_code}")
                return 0
        return 1
    except Exception as e:
        print("\033[1;31m        ########    Problem Occured While Downloading Data From Internet\033[0m")
        print(e)
        return 0
    



def grab_Last_Month():
    def find_Last_Month():
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        trip_type = "fhv_tripdata"  # Only check this trip type
        today = datetime.datetime.today()

        # Start from the current year and month
        current_year = today.year
        current_month = today.month

        while current_year >= 2020:  # Assuming data is available starting from 2020
            for month in range(current_month, 0, -1):  # Iterate months backward
                file_name = f"{trip_type}_{current_year}-{month:02}.parquet"
                url = f"{base_url}{file_name}"

                response = requests.head(url)  # Check if the file exists
                if response.status_code == 200:
                    return [current_year], [month]

            # Move to the previous year after checking all months of the current year
            current_year -= 1
            current_month = 12  # Reset to December for the previous year

        print("No available data found for 'fhv_tripdata'.")
        return None
    
    # Internal call to the helper function
    years, months = find_Last_Month()

    if years and months:
        # Call grab_Data_From_Source with the results (optional if needed here)
        grab_Data_From_Source(years=years, months=months)
