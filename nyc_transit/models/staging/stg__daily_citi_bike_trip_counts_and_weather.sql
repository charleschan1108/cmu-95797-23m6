WITH SOURCE AS (
    SELECT 
        *
    FROM
        {{ source("main", "daily_citi_bike_trip_counts_and_weather") }}
),

RENAMED AS (
    SELECT 
        date::date AS date,
        trips::int AS trips,
        precipitation::double AS precipitation,
        snowfall::double AS snowfall,
        max_temperature::double AS max_temperature,
        min_temperature::double AS min_temperature,
        replace(average_wind_speed, 'NA', NULL)::double AS average_wind_speed,
        dow::int AS dow,
        year::int AS year,
        month::int AS month,
        holiday::boolean AS is_holiday,
        replace(stations_in_service, 'NA', NULL)::int AS  stations_in_service,
        weekday::boolean AS is_weekday,
        weekday_non_holiday::boolean AS is_weekday_non_holiday,
        snow_depth::double AS snow_depth
    FROM 
        SOURCE
)

SELECT * FROM RENAMED