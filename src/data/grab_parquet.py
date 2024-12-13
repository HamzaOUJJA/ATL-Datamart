from minio import Minio
import requests
import os
import sys
from minio import Minio
from tqdm import tqdm

def main():
    #grab_Last_Month()
    write_data_minio()




def grab_Last_Month() -> None:
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    files_to_download = [ "yellow_tripdata_2024-09.parquet", "green_tripdata_2024-09.parquet", "fhv_tripdata_2024-09.parquet", "fhvhv_tripdata_2024-09.parquet",]
    
    base_dir = os.path.join("..", "..", "data", "raw")

    for file_name in files_to_download:
        url = f"{base_url}{file_name}"  # Construct the full URL
        file_path = os.path.join(base_dir, file_name)
        
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))  # Total size in bytes
            downloaded_size = 0
            
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):  # Download in 1KB chunks
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    percentage = (downloaded_size / total_size) * 100
                    print(f"\rDownloading {file_name}: {percentage:.2f}%", end="")  
            print(f"\nDownloaded: {file_path}")
        else:
            print(f"Failed to download {file_name}. Status code: {response.status_code}")



def write_data_minio():
    # MinIO client setup
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minioadmin",
        secret_key="minioadmin123"
    )

    bucket = "your-bucket-name"  # Replace with your bucket name

    # Check if the bucket exists; if not, create it
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")
    else:
        print(f"Bucket '{bucket}' already exists.")

    # Path to the folder containing the files
    raw_data_folder = os.path.abspath("../../data/raw")

    # Iterate through all files in the folder and upload them
    for root, dirs, files in os.walk(raw_data_folder):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, raw_data_folder)  # Preserve folder structure

            # Calculate file size for tracking
            file_size = os.path.getsize(file_path)

            # Use tqdm to show a progress bar
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=file) as pbar:
                try:
                    # Upload the file with fput_object and track progress manually
                    client.fput_object(bucket, object_name, file_path)
                    pbar.update(file_size)  # Update progress as the file is uploaded
                    print(f"\nUploaded: {file_path} as {object_name}")
                except Exception as e:
                    print(f"\nError uploading {file_path}: {e}")



if __name__ == '__main__':
    sys.exit(main())




"""
docker run -d -p 9000:9000 -p 9090:9090 --name minio \
  -v ~/minio/data:/data \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin123" \
  minio/minio server /data --console-address ":9090"
"""