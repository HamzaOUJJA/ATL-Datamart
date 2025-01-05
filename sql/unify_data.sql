--32
DROP TABLE IF EXISTS warehouse_data;

CREATE TABLE warehouse_data (
    "trip_id"               SERIAL PRIMARY KEY,
    "vehicul_type"          VARCHAR(50),        -- green    yellow  fhv fhvhv
    "pickup_datetime"       TIMESTAMP,          -- green    yellow  fhv fhvhv
    "dropoff_datetime"      TIMESTAMP,          -- green    yellow  fhv fhvhv
    "PULocationID"          INT,                -- green    yellow  fhv fhvhv
    "DOLocationID"          INT,                -- green    yellow  fhv fhvhv

    "trip_type"             INT,                -- green
    "ehail_fee"	            TEXT,               -- green
    "VendorID"              INT,                -- green    yellow
    "passenger_count"       INT,                -- green    yellow
    "trip_distance"         FLOAT,              -- green    yellow      fhvhv
    "extra"                 FLOAT,              -- green    yellow
    "store_and_fwd_flag"	TEXT,               -- green    yellow
    "RatecodeID"	        INT,                -- green    yellow
    "improvement_surcharge" INT,                -- green    yellow
    "payment_type"          INT,                -- green    yellow
    "total_fare"            FLOAT,              -- green    yellow      fhvhv
    "fare"                  FLOAT,              -- green    yellow      fhvhv
    "tolls_amount"          FLOAT,              -- green    yellow      fhvhv
    "mta_tax"               FLOAT,              -- green    yellow      fhvhv
    "tip_amount"            FLOAT,              -- green    yellow      fhvhv
    "congestion_surcharge"  FLOAT,              -- green    yellow      fhvhv
    "airport_fee"           FLOAT,              --          yellow      fhvhv
    "hvfhs_license_num"     VARCHAR(50),        --                      fhvhv
    "originating_base_num"  VARCHAR(50),        --                      fhvhv
    "request_datetime"      TIMESTAMP,          --                      fhvhv
    "on_scene_datetime"     TIMESTAMP,          --                      fhvhv
    "trip_time"             INT,                --                      fhvhv
    "bcf"                   FLOAT,              --                      fhvhv
    "shared_request_flag"   TEXT,               --                      fhvhv
    "shared_match_flag"     TEXT,               --                      fhvhv
    "access_a_ride_flag"	TEXT,               --                      fhvhv
    "wav_request_flag"	    TEXT,               --                      fhvhv
    "wav_match_flag"        TEXT,               --                      fhvhv
    "dispatching_base_num"  VARCHAR(50),        --                  fhv fhvhv
    "SR_Flag"               INT,                --                  fhv
    "Affiliated_base_number"VARCHAR(50)         --                  fhv
);


-- FHVHV Tripdata
INSERT INTO warehouse_data (
    "vehicul_type", 
    "hvfhs_license_num",
    "dispatching_base_num", 
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime", 
    "dropoff_datetime", 
    "PULocationID", 
    "DOLocationID", 
    "trip_distance", 
    "trip_time",
    "fare", 
    "tolls_amount", 
    "bcf",
    "mta_tax",
    "congestion_surcharge", 
    "airport_fee", 
    "tip_amount",
    "total_fare",
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag")
SELECT 
    'fhvhv', 
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime"::TIMESTAMP,
    "on_scene_datetime"::TIMESTAMP, 
    "pickup_datetime"::TIMESTAMP, 
    "dropoff_datetime"::TIMESTAMP, 
    "PULocationID", 
    "DOLocationID", 
    "trip_miles", 
    "trip_time",
    "base_passenger_fare", 
    "tolls", 
    "bcf",
    "sales_tax",
    "congestion_surcharge", 
    "airport_fee", 
    "tips", 
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag"
FROM "fhvhv_tripdata_2024-10";



-- FHV Tripdata
INSERT INTO warehouse_data (
    "vehicul_type", 
    "dispatching_base_num",
    "pickup_datetime", 
    "dropoff_datetime", 
    "PULocationID", 
    "DOLocationID", 
    "SR_Flag",
    "Affiliated_base_number")
SELECT 
    'fhv', 
    "dispatching_base_num",
    "pickup_datetime"::TIMESTAMP, 
    "dropOff_datetime"::TIMESTAMP, 
    "PUlocationID", 
    "DOlocationID", 
    "SR_Flag"::INTEGER,
    "Affiliated_base_number"
FROM "fhv_tripdata_2024-10";


-- Yellow Tripdata
INSERT INTO warehouse_data (
    "vehicul_type", 
    "VendorID", 
    "pickup_datetime", 
    "dropoff_datetime", 
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID", 
    "DOLocationID", 
    "payment_type",
    "fare", 
    "extra", 
    "mta_tax", 
    "tip_amount", 
    "tolls_amount", 
    "improvement_surcharge",
    "total_fare",
    "congestion_surcharge",   
    "airport_fee")
SELECT 
    'yellow',
    "VendorID", 
    "tpep_pickup_datetime"::TIMESTAMP, 
    "tpep_dropoff_datetime"::TIMESTAMP, 
    "passenger_count", 
    "trip_distance", 
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID", 
    "DOLocationID",
    "payment_type", 
    "fare_amount", 
    "extra", 
    "mta_tax", 
    "tip_amount", 
    "tolls_amount", 
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee"
FROM "yellow_tripdata_2024-10";




-- Green Tripdata 
INSERT INTO warehouse_data (
    "vehicul_type", "VendorID", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID", 
    "passenger_count", "trip_distance", "fare", "extra", "mta_tax", "tip_amount", "tolls_amount", 
    "ehail_fee", "improvement_surcharge", "total_fare", "payment_type", "trip_type", "congestion_surcharge")
SELECT 
    'green', "VendorID", "lpep_pickup_datetime"::TIMESTAMP, "lpep_dropoff_datetime"::TIMESTAMP, "store_and_fwd_flag", "RatecodeID", 
    "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", 
    "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type", "trip_type", "congestion_surcharge"
FROM "green_tripdata_2024-10";







DROP TABLE IF EXISTS "fhvhv_tripdata_2024-10", "fhv_tripdata_2024-10", "yellow_tripdata_2024-10", "green_tripdata_2024-10";
