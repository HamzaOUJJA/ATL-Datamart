from minio import Minio
import requests
import os
import sys

def main():
    grab_data()

def grab_data() -> None:
    """Download trip data files from the specified URL."""
    # Base URL for the trip data files
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Year and months to download
    year = 2024
    start_month = 1  # January
    end_month = 8    # August

    # File types to download
    file_types = [
        "fhvhv_tripdata_",
        "fhv_tripdata_",
        "green_tripdata_",
        "yellow_tripdata_"
    ]

    # Loop through each month from start to end month
    for month in range(start_month, end_month + 1):
        for file_type in file_types:
            # Construct the file name and URL
            file_name = f"{file_type}{year}-{month:02d}.parquet"
            file_path = os.path.join("..", "..", "data", "raw", file_name)
            url = f"{base_url}{file_name}"
            
            # Send a GET request to the URL
            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                # Open a file in write-binary mode and save the content
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Downloaded: {file_path}")
            else:
                print(f"Failed to download {url}. Status code: {response.status_code}")

def write_data_minio() -> None:
    """Put all Parquet files into Minio."""
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "NOM_DU_BUCKET_ICI"
    
    # Check if the bucket exists
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} created.")
    else:
        print(f"Bucket {bucket} already exists.")

if __name__ == '__main__':
    sys.exit(main())
