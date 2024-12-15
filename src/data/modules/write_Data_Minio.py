from minio import Minio
import os
from tqdm import tqdm








def write_Data_Minio():
    # Client MinIO 
    minioClient = Minio(
        "localhost:9000",
        secure=False,
        access_key="minioadmin",
        secret_key="minioadmin123"
    )

    bucket = "alt-datamart-bucket"  # 
    # Create the bucket if it doesn't exist
    if not minioClient.bucket_exists(bucket):
        minioClient.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")
 

    # Path to the folder containing the files
    baseDir = os.path.abspath("../../data/raw")


    # Iterate through all files in the folder and upload them
    for root, _, files in os.walk(baseDir):  # Ignore dirs by replacing it with '_'
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, baseDir)  # Preserve folder structure

            try:
                # Upload the file to MinIO with progress tracking
                minioClient.fput_object(bucket, object_name, file_path)
                print(f"\nUploaded: {file_path} as {object_name}")

            except Exception as e:
                print(f"Unexpected error uploading {file_path}: {e}")