SELECT
    DATE_PART('weekday', started_at_ts) AS day_of_week, -- get the day of week from timestamp
    COUNT(*) AS trip_cnt,
    SUM(duration_sec) AS total_duration_sec -- get total duration for all trips
FROM 
    {{ ref('mart__fact_all_bike_trips') }}
GROUP BY ALL;