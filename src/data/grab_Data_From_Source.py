########################################## 
###  This file grabs data from NewYork State at https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
###  then stores it in the local folder ../../data/raw
###  To grab last month's data use : grab_Data_From_Source([2024], [10])
###  To grab last year's data use  : grab_Data_From_Source([2024], list(range(1, 13)))
##########################################


import requests
import os



def grab_Data_From_Source(years: list = [2024], months: list = [10], trip_types: list = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]):
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
                        print(f"\r\033[35mDownloading From Internet : {file_name} : {percentage:.2f}%\033[0m", end="")
                print() 
            else:
                print(f"Failed to download {file_name}. Status code: {response.status_code}")
                return 0
        return 1
    except Exception as e:
        print("\033[1;31m ###### Problem Occured While Downloading Data From Internet ######\033[0m")
        print(e)
        return 0