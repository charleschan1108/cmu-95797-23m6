with stg_bike_trip as (
    SELECT *
    FROM {{ ref('stg__bike_data') }}
)

SELECT
    started_at_ts,
    ended_at_ts,
	datediff('minute', started_at_ts, ended_at_ts) as duration_min,
	datediff('second', started_at_ts, ended_at_ts) as duration_sec,
    start_station_id,
    end_station_id
FROM
    stg_bike_trip
