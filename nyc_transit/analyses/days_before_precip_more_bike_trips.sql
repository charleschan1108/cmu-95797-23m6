-- Methodology:
-- 1st, count number of bike trips by date using mart__fact_all_bike_trips
-- 2nd, inner join the above with stg__central_park_weather on date and station id
-- 3rd, classify the date using the following rules:
--     1. current prcp = 0 & tomorrow prcp > 0 => 'day_prior_to_prcp_or_snow'
--     2. current snow = 0 & tomorrow snow > 0 => 'day_prior_to_prcp_or_snow'
--     3. prcp > 0 or snow > 0 => 'prcp_or_snow_days'
--     4. otherwise, 'other_days'
-- Finally, group by date_type calculate the average of bike trips

with bike_data as ( 
    SELECT 
        date_trunc('day', started_at_ts)::date as date,
        start_station_id,
        COUNT(*) AS bike_trips
    FROM {{ ref('mart__fact_all_bike_trips') }} 
    GROUP BY ALL
),

 weather_bike_trips AS (
    SELECT 
        bd.date,
        bd.bike_trips,
        w.station,
        w.name,
        w.prcp,
        w.snow
    FROM bike_data bd
    INNER JOIN {{ ref('stg__central_park_weather') }}  w ON bd.date = w.date
),

weather_bike_trips_date_type AS (
    SELECT 
        station,
        name,
        date,
        prcp,
        snow,
        CASE 
            WHEN (LEAD(prcp, 1) OVER next_day > 0) AND (prcp = 0) THEN 'days_prior_to_prcp_or_snow'
            WHEN (LEAD(snow, 1) OVER next_day > 0) AND (snow = 0) THEN 'days_prior_to_prcp_or_snow'
            WHEN (prcp > 0) OR (snow > 0) THEN 'prcp_or_snow_days'
            ELSE 'days_prior_to_prcp_or_snow'
        END AS date_type,
        bike_trips
    FROM weather_bike_trips
    window next_day AS (
        ORDER BY date
    )
)

SELECT 
    date_type,
    AVG(bike_trips) as avg_bike_trips_cnt
FROM weather_bike_trips_date_type
GROUP BY ALL

