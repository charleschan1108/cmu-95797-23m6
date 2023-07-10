WITH SOURCE AS (
    SELECT
        *
    FROM 
        {{ source('main', 'green_tripdata') }}
),

/* RAW data in PARQUET format, putting data types here for reference only 
    as the data type information is preserved */
/* Data dictionary avialable at https://data.cityofnewyork.us/Transportation/2018-Green-Taxi-Trip-Data/w7fs-fd9i */
RENAMED AS (
    SELECT
        VendorID::int AS VendorID,
        lpep_pickup_datetime::datetime AS lpep_pickup_datetime,
        lpep_dropoff_datetime::datetime AS lpep_dropoff_datetime,
        {{ convert_text_to_booloean('store_and_fwd_flag') }}::boolean AS store_and_fwd_flag,
        RatecodeID::varchar AS RatecodeID,
        PULocationID::int AS PULocationID,
        DOLocationID::int AS DOLocationID,
        passenger_count::double AS passenger_count,
        trip_distance::double AS trip_distance,
        fare_amount::double AS fare_amount,
        extra::double AS extra,
        mta_tax::double AS mta_tax,
        tip_amount::double AS tip_amount,
        tolls_amount::double AS tolls_amount,
        -- ehail_fee::int AS ehail_fee -- ALL null,
        improvement_surcharge::double AS improvement_surcharge,
        total_amount::double AS total_amount,
        payment_type::double AS payment_type,
        trip_type::double AS trip_type,
        congestion_surcharge::double AS congestion_surcharge,
        filename
    FROM
        SOURCE
)

SELECT * FROM RENAMED