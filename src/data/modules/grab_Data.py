import requests
import os
from tqdm import tqdm


def grab_Data(years: list = [2024], months: list = [9]) -> None:
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    trip_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]
    files_to_download = [
        f"{trip_type}_{year}-{month:02}.parquet"
        for trip_type in trip_types
        for year in years
        for month in months
    ]
    
    base_dir = os.path.join("..", "..", "data", "raw")

    for file_name in files_to_download:
        url = f"{base_url}{file_name}" 
        year, month = file_name.split('_')[2].split('.')[0].split('-')
        folder = f"{year}/{month}"
        file_path = os.path.join(base_dir, folder, file_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))  # Total size in bytes
            downloaded_size = 0
            
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=256*1024):  # Download in 1KB chunks
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    percentage = (downloaded_size / total_size) * 100
                    print(f"\rDownloading {file_name}: {percentage:.2f}%", end="")  
            print(f"\nDownloaded: {file_path}")
        else:
            print(f"Failed to download {file_name}. Status code: {response.status_code}")