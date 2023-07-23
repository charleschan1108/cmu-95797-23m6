with stg_start_bike_station as (
    SELECT 
        start_station_id AS station_id,
        FIRST(start_station_name) AS station_name, -- Select first because the name for human reference only
        AVG(start_lat) AS station_lat, -- trying to get a closer value to the bike_data table using average
        AVG(start_lng) AS station_lng -- trying to get a closer value to the bike_data table using average
    FROM 
        {{ ref('stg__bike_data') }}
    GROUP BY
        start_station_id
),

stg_end_bike_station as (
    SELECT 
        end_station_id AS station_id,
        FIRST(end_station_name) AS station_name,
        AVG(end_lat) AS station_lat, -- trying to get a closer value to the bike_data table using average
        AVG(end_lng) AS station_lng -- trying to get a closer value to the bike_data table using average
    FROM 
        {{ ref('stg__bike_data') }}
    GROUP BY
        end_station_id
),

stg_bike_station as (
    SELECT * FROM stg_start_bike_station
    UNION ALL
    SELECT * FROM stg_end_bike_station
) -- union both to make sure all stations are covered in the dimension table

SELECT DISTINCT
    station_id,
    FIRST(station_name),
    AVG(station_lat),
    AVG(station_lng)
FROM 
    stg_bike_station
GROUP BY
    station_id