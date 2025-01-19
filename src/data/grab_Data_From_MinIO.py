########################################## 
###  This file grabs data from MinIO 
###  then stores it in the local folder ../../data/raw
##########################################

import os
from minio import Minio






def grab_Data_From_MinIO():
    print("\033[1;32m        ########    Downloading Data From MinIO!\033[0m")
    try:
        # Initialize MinIO client
        minio_client = Minio(
            "localhost:9000", secure=False, access_key="minio", secret_key="minio123"
        )
        
        # Define bucket and local directory to save the files
        bucket = "alt-datamart-bucket"
        download_dir = "../../data/raw/"
        
        # Check if the bucket exists
        if not minio_client.bucket_exists(bucket):
            print(f"Bucket '{bucket}' does not exist.")
            return
        
        # List all objects in the bucket
        objects = minio_client.list_objects(bucket, recursive=True)

        # Ensure the local directory exists
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        
        # Download each file in the bucket
        for obj in objects:
            file_path = os.path.join(download_dir, obj.object_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Create subdirectories if needed
            
            # Get the file size to track progress
            response = minio_client.get_object(bucket, obj.object_name)
            file_size = int(response.headers.get("Content-Length", 0))
            
            # Start downloading the file in chunks and track progress
            downloaded = 0
            chunk_size = 1024 * 1024  # 1 MB chunks
            with open(file_path, 'wb') as f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Calculate and display progress
                    progress = (downloaded / file_size) * 100
                    print(f"\r\033[38;5;214mDownloading From MinIO : {obj.object_name} : {progress:.2f}%\033[0m", end="")
            print()
        return 1
    except Exception as e:
        print("\033[1;31m        ########    Problem Occured While Downloading Data From MinIO\033[0m")
        print(e)
        return 0 