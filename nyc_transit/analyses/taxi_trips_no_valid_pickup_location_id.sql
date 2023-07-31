SELECT
    type,
    pickup_datetime,
    dropoff_datetime,
    duration_min,
    duration_sec,
    pulocationid,
    dolocationid,
    dim_loc.locationid, -- make sure it is null, i.e. no valid record in dim_location table
    dim_loc.borough -- make sure it is null, i.e. no valid record in dim_location table
FROM 
    {{ ref('mart__fact_all_taxi_trips') }} taxi_trip
LEFT JOIN {{ ref('mart__dim_locations') }} dim_loc ON taxi_trip.pulocationid = dim_loc.locationid
WHERE dim_loc.locationid is NULL -- keeping those trips without valid pickup location in dim_location table

