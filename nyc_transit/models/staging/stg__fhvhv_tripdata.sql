WITH SOURCE AS (
    SELECT
        *
    FROM 
        {{ source('main', 'fhvhv_tripdata') }}
),

/* RAW data in PARQUET format, putting data types here for reference only 
    as the data type information is preserved */

RENAMED AS (
    SELECT
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime::datetime AS request_datetime,
        on_scene_datetime::datetime AS on_scene_datetime,
        pickup_datetime::datetime AS pickup_datetime,
        dropoff_datetime::datetime AS dropoff_datetime,
        PULocationID::int AS PULocationID,
        trip_miles::double AS trip_miles,
        trip_time::int AS trip_time,
        base_passenger_fare::double AS base_passenger_fare,
        tolls::double AS tolls,
        bcf::double AS bcf,
        sales_tax::double AS sales_tax,
        congestion_surcharge::double AS congestion_surcharge,
        airport_fee::double AS airport_fee,
        tips::double AS tips,
        driver_pay::double AS driver_pay,
        {{ convert_text_to_booloean('shared_request_flag') }}::boolean AS shared_request_flag,
        {{ convert_text_to_booloean('shared_match_flag') }}::boolean AS shared_match_flag,
        {{ convert_text_to_booloean('access_a_ride_flag') }}::boolean AS access_a_ride_flag,
        {{ convert_text_to_booloean('wav_request_flag') }}::boolean AS wav_request_flag,
        {{ convert_text_to_booloean('wav_match_flag') }}::boolean AS wav_match_flag,
        DOLocationID::int AS DOLocationID,
        filename
    FROM 
        SOURCE
)

SELECT * FROM RENAMED