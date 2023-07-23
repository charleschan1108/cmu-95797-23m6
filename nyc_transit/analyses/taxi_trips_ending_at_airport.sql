{%- set temp_directory = './tmp.tmp' -%} -- set temp_directory to avoid out of memory error

WITH airport_service_zone AS (
    SELECT 
        LocationID::double AS LocationID,
        Borough,
        zone,
        service_zone
    FROM {{ ref('taxi_zone_lookup') }}
    WHERE service_zone in ('EWR', 'Airports') -- get EWR and Airports service zone
), 

fhv_trip_airport AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'fhv' AS type
    FROM {{ ref('stg__fhv_tripdata') }} taxi_trip
    LEFT JOIN airport_service_zone lookup ON taxi_trip.DOlocationID = lookup.LocationID
), 

fhvhv_trip_airport AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'fhvhv' AS type
    FROM {{ ref('stg__fhvhv_tripdata') }} taxi_trip
    LEFT JOIN airport_service_zone lookup ON taxi_trip.DOlocationID = lookup.LocationID
), 

green_trip_airport AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'green' AS type
    FROM {{ ref('stg__green_tripdata') }} taxi_trip
    LEFT JOIN airport_service_zone lookup ON taxi_trip.DOlocationID = lookup.LocationID
), 

yellow_trip_airport AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'yellow' AS type
    FROM {{ ref('stg__yellow_tripdata') }} taxi_trip
    LEFT JOIN airport_service_zone lookup ON taxi_trip.DOlocationID = lookup.LocationID
), 

all_airport_trip AS (
    SELECT * FROM fhv_trip_airport
    UNION ALL
    SELECT * FROM fhvhv_trip_airport
    UNION ALL
    SELECT * FROM green_trip_airport
    UNION ALL
    SELECT * FROM yellow_trip_airport
)

SELECT *
FROM all_airport_trip
UNION ALL
SELECT 
    SUM(trip_cnt) AS trip_cnt,
    'all' AS type
FROM all_airport_trip

