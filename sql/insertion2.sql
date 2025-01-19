CREATE EXTENSION dblink;
-- Establish a dblink connection to the remote server
SELECT dblink_connect('warehouse_conn', 'dbname="nyc_warehouse",
            user="postgres",
            password="admin",
            host="localhost",
            port="15432"');

-- Insert data into dim_location
INSERT INTO dim_location ("location_id", "trip_id", "PULocationID", "DOLocationID")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "location_id",  -- Assigning a surrogate key
    t."trip_id",
    t."PULocationID",
    t."DOLocationID"
FROM dblink('warehouse_conn', 'SELECT "trip_id", "PULocationID", "DOLocationID" FROM warehouse_data') 
    AS t("trip_id" INT, "PULocationID" INT, "DOLocationID" INT);

-- Insert data into dim_datetime
INSERT INTO dim_datetime ("datetime_id", "trip_id", "pickup_datetime", "dropoff_datetime", "request_datetime", "on_scene_datetime")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "datetime_id",  -- Assigning a surrogate key
    t."trip_id",
    t."pickup_datetime", 
    t."dropoff_datetime",
    t."request_datetime", 
    t."on_scene_datetime"
FROM dblink('warehouse_conn', 'SELECT "trip_id", "pickup_datetime", "dropoff_datetime", "request_datetime", "on_scene_datetime" FROM warehouse_data') 
    AS t("trip_id" INT, "pickup_datetime" TIMESTAMP, "dropoff_datetime" TIMESTAMP, "request_datetime" TIMESTAMP, "on_scene_datetime" TIMESTAMP);

-- Insert data into dim_fare
INSERT INTO dim_fare ("fare_id", "trip_id", "total_fare", "fare", "improvement_surcharge", "extra", "tolls_amount", "mta_tax", "tip_amount", "congestion_surcharge", "airport_fee", "ehail_fee", "bcf", "payment_type")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "fare_id",  -- Assigning a surrogate key
    t."trip_id",
    t."total_fare",
    t."fare", 
    t."improvement_surcharge", 
    t."extra", 
    t."tolls_amount", 
    t."mta_tax", 
    t."tip_amount", 
    t."congestion_surcharge", 
    t."airport_fee", 
    t."ehail_fee",
    t."bcf",
    t."payment_type"
FROM dblink('warehouse_conn', 'SELECT "trip_id", "total_fare", "fare", "improvement_surcharge", "extra", "tolls_amount", "mta_tax", "tip_amount", "congestion_surcharge", "airport_fee", "ehail_fee", "bcf", "payment_type" FROM warehouse_data') 
    AS t("trip_id" INT, "total_fare" NUMERIC, "fare" NUMERIC, "improvement_surcharge" NUMERIC, "extra" NUMERIC, "tolls_amount" NUMERIC, "mta_tax" NUMERIC, "tip_amount" NUMERIC, "congestion_surcharge" NUMERIC, "airport_fee" NUMERIC, "ehail_fee" NUMERIC, "bcf" NUMERIC, "payment_type" TEXT);

-- Insert data into dim_trip_details
INSERT INTO dim_trip_details ("details_id", "trip_id", "trip_type", "VendorID", "passenger_count", "trip_distance", "RatecodeID", "trip_time")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "details_id",  -- Assigning a surrogate key
    t."trip_id",
    t."trip_type", 
    t."VendorID", 
    t."passenger_count", 
    t."trip_distance", 
    t."RatecodeID", 
    t."trip_time"
FROM dblink('warehouse_conn', 'SELECT "trip_id", "trip_type", "VendorID", "passenger_count", "trip_distance", "RatecodeID", "trip_time" FROM warehouse_data') 
    AS t("trip_id" INT, "trip_type" TEXT, "VendorID" INT, "passenger_count" INT, "trip_distance" NUMERIC, "RatecodeID" INT, "trip_time" INTERVAL);

-- Insert data into dim_flags
INSERT INTO dim_flags ("flags_id", "trip_id", "shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "store_and_fwd_flag", "SR_Flag")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "flags_id",  -- Assigning a surrogate key
    t."trip_id",
    t."shared_request_flag", 
    t."shared_match_flag", 
    t."access_a_ride_flag", 
    t."wav_request_flag", 
    t."wav_match_flag", 
    t."store_and_fwd_flag", 
    t."SR_Flag"
FROM dblink('warehouse_conn', 'SELECT "trip_id", "shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "store_and_fwd_flag", "SR_Flag" FROM warehouse_data') 
    AS t("trip_id" INT, "shared_request_flag" BOOLEAN, "shared_match_flag" BOOLEAN, "access_a_ride_flag" BOOLEAN, "wav_request_flag" BOOLEAN, "wav_match_flag" BOOLEAN, "store_and_fwd_flag" BOOLEAN, "SR_Flag" BOOLEAN);

-- Insert data into dim_base
INSERT INTO dim_base ("base_id", "trip_id", "hvfhs_license_num", "originating_base_num", "dispatching_base_num", "Affiliated_base_number")
SELECT DISTINCT
    ROW_NUMBER() OVER () AS "base_id",  -- Assigning a surrogate key
    t."trip_id",
    t."hvfhs_license_num", 
    t."originating_base_num", 
    t."dispatching_base_num", 
    t."Affiliated_base_number"
FROM dblink('warehouse_conn', 'SELECT "trip_id", "hvfhs_license_num", "originating_base_num", "dispatching_base_num", "Affiliated_base_number" FROM warehouse_data') 
    AS t("trip_id" INT, "hvfhs_license_num" TEXT, "originating_base_num" TEXT, "dispatching_base_num" TEXT, "Affiliated_base_number" TEXT);

-- Insert data into fact_trip
INSERT INTO fact_trip ("trip_id", "vehicul_type", "location_id", "datetime_id", "fare_id", "details_id", "flags_id", "base_id")
SELECT
    t."trip_id",
    t."vehicul_type",
    (SELECT "location_id"   FROM dim_location WHERE "trip_id" = t."trip_id" LIMIT 1) AS "location_id",
    (SELECT "datetime_id"   FROM dim_datetime WHERE "trip_id" = t."trip_id" LIMIT 1) AS "datetime_id",
    (SELECT "fare_id"       FROM dim_fare WHERE "trip_id" = t."trip_id" LIMIT 1) AS "fare_id",
    (SELECT "details_id"    FROM dim_trip_details WHERE "trip_id" = t."trip_id" LIMIT 1) AS "details_id",
    (SELECT "flags_id"      FROM dim_flags WHERE "trip_id" = t."trip_id" LIMIT 1) AS "flags_id",
    (SELECT "base_id"       FROM dim_base WHERE "trip_id" = t."trip_id" LIMIT 1) AS "base_id"
FROM dblink('warehouse_conn', 'SELECT * FROM warehouse_data') 
    AS t("trip_id" INT, "vehicul_type" TEXT);