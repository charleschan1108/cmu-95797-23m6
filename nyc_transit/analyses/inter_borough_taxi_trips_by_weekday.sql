{%- set temp_directory = './tmp.tmp' -%} -- set temp_directory to avoid out of memory error

WITH taxi_zone AS (
    SELECT 
        LocationID::double AS LocationID, -- LocationID is storted as double in other tables
        Borough,
        zone,
        service_zone
    FROM {{ ref('taxi_zone_lookup') }}
), 

fhv_trip AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'fhv' AS type,
        weekday(pickup_datetime) AS day_of_week, -- get day of week from timestamp
        l.Borough AS start_borough, -- get start borough
        l2.Borough AS end_borough -- get detination borough
    FROM {{ ref('stg__fhv_tripdata') }} t LEFT JOIN 
    taxi_zone l ON t.PUlocationID = l.LocationID LEFT JOIN
    taxi_zone l2 ON t.DOlocationID = l2.LocationID
    GROUP BY ALL
), 

fhvhv_trip AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'fhvhv' AS type,
        weekday(pickup_datetime) AS day_of_week,
        l.Borough AS start_borough,
        l2.Borough AS end_borough
    FROM {{ ref('stg__fhvhv_tripdata') }} t LEFT JOIN 
    taxi_zone l ON t.PUlocationID = l.LocationID LEFT JOIN
    taxi_zone l2 ON t.DOlocationID = l2.LocationID
    GROUP BY ALL
), 

green_trip AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'green' AS type,
        weekday(lpep_pickup_datetime),
        l.Borough AS start_borough,
        l2.Borough AS end_borough
    FROM {{ ref('stg__green_tripdata') }} t LEFT JOIN 
    taxi_zone l ON t.PUlocationID = l.LocationID LEFT JOIN
    taxi_zone l2 ON t.DOlocationID = l2.LocationID
    GROUP BY ALL
), 

yellow_trip AS (
    SELECT
        COUNT(*) AS trip_cnt,
        'yellow' AS type,
        weekday(tpep_pickup_datetime),
        l.Borough AS start_borough,
        l2.Borough AS end_borough
    FROM {{ ref('stg__yellow_tripdata') }} t LEFT JOIN 
    taxi_zone l ON t.PUlocationID = l.LocationID LEFT JOIN
    taxi_zone l2 ON t.DOlocationID = l2.LocationID
    GROUP BY ALL
), 

all_trip AS (
    SELECT * FROM fhv_trip
    UNION ALL
    SELECT * FROM fhvhv_trip
    UNION ALL
    SELECT * FROM green_trip
    UNION ALL
    SELECT * FROM yellow_trip
),

all_trip_agg AS (
    SELECT
        SUM(trip_cnt) AS trip_cnt,
        start_borough,
        end_borough
    FROM all_trip
    GROUP BY ALL
)


SELECT 
    trip_cnt,
    trip_cnt / SUM(trip_cnt) OVER () AS PERCENT,
    start_borough,
    end_borough
FROM all_trip_agg

