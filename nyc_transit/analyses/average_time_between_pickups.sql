WITH time_difference AS (
    SELECT
        datediff('seconds', tt.pickup_datetime, lead(tt.pickup_datetime, 1,0) 
            OVER (PARTITION BY dim_loc.zone ORDER BY tt.pickup_datetime ASC))
            AS time_diff, -- calculate the difference between the pickup time of the current and next trip in the zone
            dim_loc.zone AS zone
    FROM {{ ref('mart__fact_all_taxi_trips') }} tt 
    LEFT JOIN {{ ref('mart__dim_locations') }} dim_loc ON tt.pulocationid = dim_loc.locationid
)

SELECT 
    zone,
    AVG(time_diff) AS avg_time_between_pickups
FROM time_difference 
GROUP BY ALL

