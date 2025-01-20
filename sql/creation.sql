DROP TABLE IF EXISTS fact_trip, dim_location, dim_datetime, dim_flags, dim_base, dim_trip_details, dim_fare, warehouse_data;

CREATE TABLE fact_trip (
    "trip_id"               SERIAL PRIMARY KEY,  
    "vehicul_type"          TEXT,
    "location_id"           INT,
    "datetime_id"           INT,         
    "fare_id"               INT, 
    "details_id"            INT,    
    "flags_id"              INT,
    "base_id"               INT
);
CREATE TABLE dim_location (
    "location_id"           INT PRIMARY KEY,
    "trip_id"               INT,
    "PULocationID"          TEXT,
    "DOLocationID"          TEXT
);
CREATE TABLE dim_datetime (
    "datetime_id"           INT PRIMARY KEY,
    "trip_id"               INT,
    "pickup_datetime"       TEXT,           
    "dropoff_datetime"      TEXT,
    "request_datetime"      TEXT,          --                      fhvhv
    "on_scene_datetime"     TEXT           --                      fhvhv
);
CREATE TABLE dim_fare ( 
    "fare_id"               INT PRIMARY KEY, 
    "trip_id"               INT,  
    "total_fare"            TEXT,              -- green    yellow      fhvhv
    "fare"                  TEXT,              -- green    yellow      fhvhv
    "improvement_surcharge" TEXT,                -- green    yellow
    "extra"                 TEXT,              -- green    yellow
    "tolls_amount"          TEXT,              -- green    yellow      fhvhv
    "mta_tax"               TEXT,              -- green    yellow      fhvhv
    "tip_amount"            TEXT,              -- green    yellow      fhvhv
    "congestion_surcharge"  TEXT,              -- green    yellow      fhvhv
    "airport_fee"           TEXT,              --          yellow      fhvhv
    "ehail_fee"	            TEXT,                -- green
    "bcf"                   TEXT,              --                      fhvhv
    "payment_type"          TEXT                 -- green    yellow
);
CREATE TABLE dim_trip_details (  
    "details_id"            INT PRIMARY KEY,
    "trip_id"               INT,
    "trip_type"             TEXT,                -- green 
    "VendorID"              TEXT,                -- green    yellow
    "passenger_count"       TEXT,                -- green    yellow
    "trip_distance"         TEXT,              -- green    yellow      fhvhv
    "RatecodeID"	        TEXT,                -- green    yellow
    "trip_time"             TEXT                 --                      fhvhv
);
CREATE TABLE dim_flags (
    "flags_id"              INT PRIMARY KEY,
    "trip_id"               INT,
    "shared_request_flag"   TEXT,               --                      fhvhv
    "shared_match_flag"     TEXT,               --                      fhvhv
    "access_a_ride_flag"	TEXT,               --                      fhvhv
    "wav_request_flag"	    TEXT,               --                      fhvhv
    "wav_match_flag"        TEXT,               --                      fhvhv
    "store_and_fwd_flag"	TEXT,               -- green    yellow
    "SR_Flag"               TEXT                 --                  fhv
);
CREATE TABLE dim_base (
    "base_id"               INT PRIMARY KEY,
    "trip_id"               INT,
    "hvfhs_license_num"     TEXT,        --                      fhvhv
    "originating_base_num"  TEXT,        --                      fhvhv
    "dispatching_base_num"  TEXT,        --                  fhv fhvhv
    "Affiliated_base_number"TEXT         --                  fhv
);




