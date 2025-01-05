-- Insert data into dim_location
INSERT INTO dim_location ("location_id", "trip_id", "PULocationID", "DOLocationID")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "location_id",  -- Assigning a surrogate key
    "trip_id",
    "PULocationID", 
    "DOLocationID"
FROM warehouse_data;

-- Insert data into dim_datetime
INSERT INTO dim_datetime ("datetime_id", "trip_id", "pickup_datetime", "dropoff_datetime", "request_datetime", "on_scene_datetime")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "datetime_id",  -- Assigning a surrogate key
    "trip_id",
    "pickup_datetime", 
    "dropoff_datetime",
    "request_datetime", 
    "on_scene_datetime"
FROM warehouse_data;

-- Insert data into dim_fare
INSERT INTO dim_fare ("fare_id", "trip_id", "total_fare", "fare", "improvement_surcharge", "extra", "tolls_amount", "mta_tax", "tip_amount", "congestion_surcharge", "airport_fee", "ehail_fee", "bcf", "payment_type")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "fare_id",  -- Assigning a surrogate key
    "trip_id",
    "total_fare",
    "fare", 
    "improvement_surcharge", 
    "extra", 
    "tolls_amount", 
    "mta_tax", 
    "tip_amount", 
    "congestion_surcharge", 
    "airport_fee", 
    "ehail_fee",
    "bcf",
    "payment_type"
FROM warehouse_data;

-- Insert data into dim_trip_details
INSERT INTO dim_trip_details ("details_id", "trip_id", "trip_type", "VendorID", "passenger_count", "trip_distance", "RatecodeID", "trip_time")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "details_id",  -- Assigning a surrogate key
    "trip_id",
    "trip_type", 
    "VendorID", 
    "passenger_count", 
    "trip_distance", 
    "RatecodeID", 
    "trip_time"
FROM warehouse_data;

-- Insert data into dim_flags
INSERT INTO dim_flags ("flags_id", "trip_id", "shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "store_and_fwd_flag", "SR_Flag")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "flags_id",  -- Assigning a surrogate key
    "trip_id",
    "shared_request_flag", 
    "shared_match_flag", 
    "access_a_ride_flag", 
    "wav_request_flag", 
    "wav_match_flag", 
    "store_and_fwd_flag", 
    "SR_Flag"
FROM warehouse_data;

-- Insert data into dim_base
INSERT INTO dim_base ("base_id", "trip_id", "hvfhs_license_num", "originating_base_num", "dispatching_base_num", "Affiliated_base_number")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "base_id",  -- Assigning a surrogate key
    "trip_id",
    "hvfhs_license_num", 
    "originating_base_num", 
    "dispatching_base_num", 
    "Affiliated_base_number"
FROM warehouse_data;






-- Insert data into fact_trip
INSERT INTO fact_trip ("trip_id", "vehicul_type", "location_id", "datetime_id", "fare_id", "details_id", "flags_id", "base_id")
SELECT
    "trip_id",
    "vehicul_type",
    (SELECT "location_id"   FROM dim_location WHERE "trip_id" = wd."trip_id" LIMIT 1) AS "location_id",
    (SELECT "datetime_id"   FROM dim_datetime WHERE "trip_id" = wd."trip_id" LIMIT 1) AS "datetime_id",
    (SELECT "fare_id"       FROM dim_fare WHERE "trip_id" = wd."trip_id" LIMIT 1) AS "fare_id",
    (SELECT "details_id"    FROM dim_trip_details WHERE "trip_id" = wd."trip_id"  LIMIT 1) AS "details_id",
    (SELECT "flags_id"      FROM dim_flags WHERE "trip_id" = wd."trip_id" LIMIT 1) AS "flags_id",
    (SELECT "base_id"       FROM dim_base WHERE "trip_id" = wd."trip_id" LIMIT 1) AS "base_id"
FROM warehouse_data wd;
