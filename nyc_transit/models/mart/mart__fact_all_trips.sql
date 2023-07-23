WITH fact_all_bike_trips AS (
    SELECT 
        'bike' AS type,
        started_at_ts,
        ended_at_ts,
        duration_min,
        duration_sec
    FROM {{ ref('mart__fact_all_bike_trips') }}
),

fact_all_taxi_trips AS (
    SELECT 
        'taxi' AS type,
        pickup_datetime AS started_at_ts,
        dropoff_datetime AS ended_at_ts,
        duration_min,
        duration_sec
    FROM {{ ref('mart__fact_all_taxi_trips') }}
)

SELECT *
FROM fact_all_bike_trips 
UNION ALL
SELECT *
FROM fact_all_taxi_trips 


