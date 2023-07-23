with stg_bike_trip as (
    SELECT *
    FROM {{ ref('stg__bike_data') }}
)

SELECT
    started_at_ts,
    ended_at_ts,
    (tripduration / 60 )::int AS duration_min, -- tripduration is in second, divide by 60 to get to minutes
    tripduration AS duration_sec,
    start_station_id,
    end_station_id
FROM
    stg_bike_trip
