{%- set temp_directory = './tmp.tmp' -%} -- set temp_directory to avoid out of memory error

WITH trips_by_location AS ( -- First count the number of trips for each group of pick up and drop off location ids
    SELECT
        pulocationid,
        dolocationid,
        count(*) as trips,
        sum(duration_sec) as duration_sec,
        sum(duration_min) as duration_min
    FROM
        {{ ref('mart__fact_all_taxi_trips') }}
    GROUP BY ALL

), dim_locations AS (
    SELECT
        locationid,
        borough,
        zone
    FROM
        {{ ref('mart__dim_locations') }}
)

SELECT
    trip_loc.pulocationid,
    trip_loc.dolocationid,
    pu_loc.borough as puborough,
    do_loc.borough as doborough,
    pu_loc.zone as puzone,
    do_loc.zone as dozone,
    trip_loc.trips,
    trip_loc.duration_sec,
    trip_loc.duration_min
FROM trips_by_location AS trip_loc 
LEFT JOIN dim_locations pu_loc ON trip_loc.pulocationid = pu_loc.locationid
LEFT JOIN dim_locations do_loc ON trip_loc.dolocationid = do_loc.locationid