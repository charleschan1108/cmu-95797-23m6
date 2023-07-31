{%- set temp_directory = './tmp.tmp' -%} -- set temp_directory to avoid out of memory error

-- Calculate the number of trips and average duration by all borough and zone pairs in the data

SELECT 
    puborough,
    doborough,
    puzone,
    dozone,
    trips,
    duration_min / trips AS avg_trip_duration_min,
    duration_sec / trips AS avg_trip_duration_min
FROM {{ ref('mart__fact_trips_by_borough') }}

-- by origin borough and zone, sql:
-- SELECT 
--     puborough,
--     puzone,
--     SUM(trips) as trip_num,
--     SUM(duration_min) / SUM(trips) AS avg_trip_duration_min,
--     SUM(duration_sec) / SUM(trips) AS avg_trip_duration_min
-- FROM {{ ref('mart__fact_trips_by_borough') }}

-- by destination borough and zone, sql:
-- SELECT 
--     doborough,
--     dozone,
--     SUM(trips) as trip_num,
--     SUM(duration_min) / SUM(trips) AS avg_trip_duration_min,
--     SUM(duration_sec) / SUM(trips) AS avg_trip_duration_min
-- FROM {{ ref('mart__fact_trips_by_borough') }}