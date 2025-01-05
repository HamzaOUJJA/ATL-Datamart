DROP TABLE IF EXISTS fact_trip, dim_location, dim_datetime, dim_flags, dim_base, dim_trip_details, dim_fare;

CREATE TABLE fact_trip (
    "trip_id"               SERIAL PRIMARY KEY,  
    "vehicul_type"          VARCHAR(50),
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
    "PULocationID"          INT,
    "DOLocationID"          INT
);
CREATE TABLE dim_datetime (
    "datetime_id"           INT PRIMARY KEY,
    "trip_id"               INT,
    "pickup_datetime"       TIMESTAMP,           
    "dropoff_datetime"      TIMESTAMP,
    "request_datetime"      TIMESTAMP,          --                      fhvhv
    "on_scene_datetime"     TIMESTAMP           --                      fhvhv
);
CREATE TABLE dim_fare ( 
    "fare_id"               INT PRIMARY KEY, 
    "trip_id"               INT,  
    "total_fare"            FLOAT,              -- green    yellow      fhvhv
    "fare"                  FLOAT,              -- green    yellow      fhvhv
    "improvement_surcharge" INT,                -- green    yellow
    "extra"                 FLOAT,              -- green    yellow
    "tolls_amount"          FLOAT,              -- green    yellow      fhvhv
    "mta_tax"               FLOAT,              -- green    yellow      fhvhv
    "tip_amount"            FLOAT,              -- green    yellow      fhvhv
    "congestion_surcharge"  FLOAT,              -- green    yellow      fhvhv
    "airport_fee"           FLOAT,              --          yellow      fhvhv
    "ehail_fee"	            TEXT,                -- green
    "bcf"                   FLOAT,              --                      fhvhv
    "payment_type"          INT                 -- green    yellow
);
CREATE TABLE dim_trip_details (  
    "details_id"            INT PRIMARY KEY,
    "trip_id"               INT,
    "trip_type"             INT,                -- green 
    "VendorID"              INT,                -- green    yellow
    "passenger_count"       INT,                -- green    yellow
    "trip_distance"         FLOAT,              -- green    yellow      fhvhv
    "RatecodeID"	        INT,                -- green    yellow
    "trip_time"             INT                 --                      fhvhv
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
    "SR_Flag"               INT                 --                  fhv
);
CREATE TABLE dim_base (
    "base_id"               INT PRIMARY KEY,
    "trip_id"               INT,
    "hvfhs_license_num"     VARCHAR(50),        --                      fhvhv
    "originating_base_num"  VARCHAR(50),        --                      fhvhv
    "dispatching_base_num"  VARCHAR(50),        --                  fhv fhvhv
    "Affiliated_base_number"VARCHAR(50)         --                  fhv
);




