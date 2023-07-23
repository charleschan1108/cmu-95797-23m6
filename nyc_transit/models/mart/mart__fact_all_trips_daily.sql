WITH fact_all_bike_trips AS (
    SELECT 
        'bike' AS type,
        DATE_TRUNC('day', started_at_ts) AS date, -- get date so we can group data into daily level
        *
    FROM {{ ref('mart__fact_all_bike_trips') }}
), 

fact_all_bike_trips_daily AS ( -- summarize bike trips on daily level
    SELECT 
        'bike' AS type,
        date,
        COUNT(*) AS trips,
        AVG(duration_min) AS average_trip_duration_min
    FROM 
        fact_all_bike_trips
    GROUP BY
        date
),

fact_all_taxi_trips AS (
    SELECT 
        'taxi' AS type,
        DATE_TRUNC('day', pickup_datetime) AS date,
        *
    FROM {{ ref('mart__fact_all_taxi_trips') }}
),

fact_all_taxi_trips_daily AS ( -- summarize taxi trips on daily level
    SELECT 
        'taxi' AS type,
        date,
        COUNT(*) AS trips,
        AVG(duration_min) AS average_trip_duration_min
    FROM 
        fact_all_taxi_trips
    GROUP BY
        date
)

SELECT *
FROM fact_all_bike_trips_daily 
UNION ALL
SELECT *
FROM fact_all_taxi_trips_daily 
