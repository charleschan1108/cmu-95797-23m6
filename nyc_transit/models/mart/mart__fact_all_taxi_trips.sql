with stg_fhv_tripdata AS (
    SELECT 
        pickup_datetime,
        dropOff_datetime AS dropoff_datetime,
        datediff('seconds', pickup_datetime, dropOff_datetime)::int AS tripduration,
        PUlocationID,
        DOlocationID
    FROM {{ ref('stg__fhv_tripdata') }}
), 

stg_fhvhv_tripdata AS (
    SELECT 
        pickup_datetime,
        dropoff_datetime,
        datediff('seconds', pickup_datetime, dropoff_datetime)::int AS tripduration,
        PUlocationID,
        DOlocationID
    FROM {{ ref('stg__fhvhv_tripdata') }}
),

stg_green_tripdata AS (
    SELECT 
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        datediff('seconds', lpep_pickup_datetime, lpep_dropoff_datetime)::int AS tripduration,
        PUlocationID,
        DOlocationID
    FROM {{ ref('stg__green_tripdata') }}
),

stg_yellow_tripdata AS (
    SELECT 
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        datediff('seconds', tpep_pickup_datetime, tpep_dropoff_datetime)::int AS tripduration,
        PUlocationID,
        DOlocationID
    FROM {{ ref('stg__yellow_tripdata') }}
),

stg_tripdata AS (
    SELECT 
        'fhv' AS type,
        (tripduration / 60)::int as duration_min,
        tripduration::int AS duration_sec,
        pickup_datetime,
        dropoff_datetime,
        PUlocationID,
        DOlocationID
    FROM 
        stg_fhv_tripdata
    UNION
    SELECT 
        'fhvhv' AS type,
        (tripduration / 60)::int as duration_min,
        tripduration::int AS duration_sec,
        pickup_datetime,
        dropoff_datetime,
        PUlocationID,
        DOlocationID
    FROM 
        stg_fhvhv_tripdata
    UNION
    SELECT 
        'green' AS type,
        (tripduration / 60)::int as duration_min,
        tripduration::int AS duration_sec,
        pickup_datetime,
        dropoff_datetime,
        PUlocationID,
        DOlocationID
    FROM 
        stg_green_tripdata
    UNION
    SELECT 
        'yellow' AS type,
        (tripduration / 60)::int as duration_min,
        tripduration::int AS duration_sec,
        pickup_datetime,
        dropoff_datetime,
        PUlocationID,
        DOlocationID
    FROM 
        stg_yellow_tripdata
)

SELECT 
    type,
    pickup_datetime,
    dropoff_datetime,
    duration_min,
    duration_sec,
    pulocationid,
    dolocationid
FROM
    stg_tripdata