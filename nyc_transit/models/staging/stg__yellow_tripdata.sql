WITH SOURCE AS (
    SELECT
        *
    FROM 
        {{ source('main', 'yellow_tripdata') }}
),

/* RAW data in PARQUET format, putting data types here for reference only 
    as the data type information is preserved */
/* Data dictionary avialable at https://data.cityofnewyork.us/Transportation/2018-Green-Taxi-Trip-Data/w7fs-fd9i */
RENAMED AS (
    SELECT
        VendorID::int AS VendorID,
        tpep_pickup_datetime::datetime AS tpep_pickup_datetime,
        tpep_dropoff_datetime::datetime AS tpep_dropoff_datetime,
        passenger_count::double AS passenger_count,
        trip_distance::double AS trip_distance,
        RatecodeID::double AS RatecodeID,
        {{ convert_text_to_booloean('store_and_fwd_flag') }}::boolean AS store_and_fwd_flag,
        PULocationID::int AS PULocationID,
        DOLocationID::int AS DOLocationID,
        payment_type::double AS payment_type,
        fare_amount::double AS fare_amount,
        extra::double AS extra,
        mta_tax::double AS mta_tax,
        tip_amount::double AS tip_amount,
        tolls_amount::double AS tolls_amount,
        improvement_surcharge::double AS improvement_surcharge,
        total_amount::double AS total_amount,
        congestion_surcharge::double AS congestion_surcharge,
        airport_fee::double AS airport_fee,
        filename
    FROM
        SOURCE
)

SELECT * FROM RENAMED