--37
DROP TABLE IF EXISTS warehouse_data;

CREATE TABLE warehouse_data (
    "trip_id"               SERIAL PRIMARY KEY,
    "vehicul_type"          TEXT,                   -- green    yellow  fhv fhvhv
    "pickup_datetime"       TEXT,                   -- green    yellow  fhv fhvhv
    "dropoff_datetime"      TEXT,                   -- green    yellow  fhv fhvhv
    "PULocationID"          TEXT,                   -- green    yellow  fhv fhvhv
    "DOLocationID"          TEXT,                   -- green    yellow  fhv fhvhv

    "trip_type"             TEXT,                   -- green
    "ehail_fee"	            TEXT,                   -- green
    "VendorID"              TEXT,                   -- green    yellow
    "passenger_count"       TEXT,                   -- green    yellow
    "trip_distance"         TEXT,                   -- green    yellow      fhvhv
    "extra"                 TEXT,                   -- green    yellow
    "store_and_fwd_flag"	TEXT,                   -- green    yellow
    "RatecodeID"	        TEXT,                   -- green    yellow
    "improvement_surcharge" TEXT,                   -- green    yellow
    "payment_type"          TEXT,                   -- green    yellow
    "total_fare"            TEXT,                   -- green    yellow      fhvhv
    "fare"                  TEXT,                   -- green    yellow      fhvhv
    "tolls_amount"          TEXT,                   -- green    yellow      fhvhv
    "mta_tax"               TEXT,                   -- green    yellow      fhvhv
    "tip_amount"            TEXT,                   -- green    yellow      fhvhv
    "congestion_surcharge"  TEXT,                   -- green    yellow      fhvhv
    "airport_fee"           TEXT,                   --          yellow      fhvhv
    "hvfhs_license_num"     TEXT,                   --                      fhvhv
    "originating_base_num"  TEXT,                   --                      fhvhv
    "request_datetime"      TEXT,                   --                      fhvhv
    "on_scene_datetime"     TEXT,                   --                      fhvhv
    "trip_time"             TEXT,                   --                      fhvhv
    "bcf"                   TEXT,                   --                      fhvhv
    "shared_request_flag"   TEXT,                   --                      fhvhv
    "shared_match_flag"     TEXT,                   --                      fhvhv
    "access_a_ride_flag"	TEXT,                   --                      fhvhv
    "wav_request_flag"	    TEXT,                   --                      fhvhv
    "wav_match_flag"        TEXT,                   --                      fhvhv
    "dispatching_base_num"  TEXT,                   --                  fhv fhvhv
    "SR_Flag"               TEXT,                   --                  fhv
    "Affiliated_base_number"TEXT                    --                  fhv
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
    "request_datetime",
    "on_scene_datetime", 
    "pickup_datetime", 
    "dropoff_datetime", 
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
    "pickup_datetime", 
    "dropOff_datetime", 
    "PUlocationID", 
    "DOlocationID", 
    "SR_Flag",
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
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime", 
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
    'green', "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "RatecodeID", 
    "PULocationID", "DOLocationID", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", 
    "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type", "trip_type", "congestion_surcharge"
FROM "green_tripdata_2024-10";







DROP TABLE IF EXISTS "fhvhv_tripdata_2024-10", "fhv_tripdata_2024-10", "yellow_tripdata_2024-10", "green_tripdata_2024-10";
