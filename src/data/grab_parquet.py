import os
from modules.grab_Data import grab_Data
from modules.write_Data_Minio import write_Data_Minio
import sys




def main():
    grab_Data([2024], [8,9])
    #grab_Data([2024], list(range(1, 13)))
    write_Data_Minio()
    
  




if __name__ == '__main__':
    sys.exit(main())



# Commande pour lancer minio
"""
docker run -d -p 9000:9000 -p 9090:9090 --name minio \
  -v ~/minio/data:/data \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin123" \
  minio/minio server /data --console-address ":9090"
"""