-- set temp_directory to avoid out of memory error
{% set temp_directory = '/home/charles/app/DW/Assignments/cmu-95797-23m6/sql/tmp.tmp' %}
{% set memory_limit='4GB' %}

SELECT
    dim_loc.borough, 
    dim_loc.zone,
    fare_amount,
    AVG(fare_amount) OVER (PARTITION BY dim_loc.zone) AS zone_avg_fare_amount,
    AVG(fare_amount) OVER (PARTITION BY dim_loc.borough) AS borough_avg_fare_amount,
    AVG(fare_amount) OVER () AS overall_avg_fare_amount
FROM {{ source('main', 'yellow_tripdata') }} yt
LEFT JOIN {{ ref('mart__dim_locations') }} dim_loc ON yt.pulocationid = dim_loc.locationid
