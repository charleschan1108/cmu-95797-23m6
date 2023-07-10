WITH SOURCE1 AS (
    SELECT 
          tripduration,
          starttime,
          stoptime,
          "start station id",
          "start station name",
          "start station latitude",
          "start station longitude",
          "end station id",
          "end station name",
          "end station latitude",
          "end station longitude",
          bikeid,
          usertype,
          "birth year",
          gender,
          filename
   FROM {{ source("main", "bike_data") }}
),
     SOURCE2 AS (
        SELECT 
          ride_id,
          rideable_type,
          started_at,
          ended_at,
          start_station_name,
          start_station_id,
          end_station_name,
          end_station_id,
          start_lat,
          start_lng,
          end_lat,
          end_lng,
          member_casual,
          filename
    FROM 
        {{ source("main", "bike_data") }}
),
     RENAMED1 AS (
    SELECT 
          NULL::varchar AS ride_id, -- New schema
          NULL::varchar AS rideable_type, -- New schema
          tripduration::int AS trip_duration,
          LEFT(starttime::datetime, 19) AS started_at, -- standardise timestamp format
          LEFT(stoptime::datetime, 19) AS ended_at, -- standardise timestamp format
          "start station id"::VARCHAR AS start_station_id,
          "start station name" AS start_station_name,
          "start station latitude"::double AS start_lat,
          "start station longitude"::double AS start_lng,
          "end station id"::VARCHAR AS end_station_id,
          "end station name" AS end_station_name,
          "end station latitude"::double AS end_lat,
          "end station longitude"::double AS end_lng,
          bikeid::int AS bike_id,
          usertype AS user_type,
          "birth year"::int AS birth_year,
          gender::int AS gender,
          NULL::varchar AS member_casual,
          filename
    FROM
        SOURCE1
  ), 
     RENAMED2 AS (
    SELECT 
          ride_id,
          rideable_type,
          date_part('second', ended_at::datetime - started_at::datetime) AS trip_duration, -- Old Schema
          left(started_at::datetime, 19) AS started_at, -- standardise timestamp format
          left(ended_at::datetime, 19) AS ended_at, -- standardise timestamp format
          start_station_id::varchar AS start_station_id,
          start_station_name,
          start_lat::double AS start_lat,
          start_lng::double AS start_lng,
          end_station_id::varchar AS end_station_id,
          end_station_name,
          end_lat::double AS end_lat,
          end_lng::double AS end_lng,
          NULL::int AS bike_id, -- Old Schema
          NULL AS user_type, -- Old Schema
          NULL::int AS birth_year, -- Old Schema
          NULL::int AS gender, -- Old Schema
          member_casual,
          filename
   FROM SOURCE2
)

SELECT * FROM RENAMED1 
UNION ALL 
SELECT * FROM RENAMED2