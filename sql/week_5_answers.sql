SET temp_directory = './tmp.tmp';
-- SET memory_limit='6GB';
-- SET threads TO 6;

-- print scripts
.echo ON

-- show all results
.maxrows 100

-- Q1 trips_duration_grouped_by_borough_zone.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/trips_duration_grouped_by_borough_zone.sql

-- Q2 taxi_trips_no_valid_pickup_location_id.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/taxi_trips_no_valid_pickup_location_id.sql

-- Q3 zones_with_less_than_100k_trips.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/zones_with_less_than_100k_trips.sql

-- Q4 pivot_trips_by_borough.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/pivot_trips_by_borough.sql

-- Q5 yellow_taxi_fare_comparison.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/yellow_taxi_fare_comparison.sql

-- Q6 dedupe.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/dedupe.sql

-- Q7  seven_day_moving_average_prcp.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/seven_day_moving_average_prcp.sql

-- Q8 seven_day_moving_aggs_weather.sql
.once ../answers/seven_day_moving_aggs_weather.txt
.read ../nyc_transit/target/compiled/nyc_transit/analyses/seven_day_moving_aggs_weather.sql

-- Q9 average_time_between_pickups.sql
.once average_time_between_pickups.txt
.read ../nyc_transit/target/compiled/nyc_transit/analyses/average_time_between_pickups.sql

-- Q10 days_before_precip_more_bike_trips.sql
.once days_before_precip_more_bike_trips.txt
.read ../nyc_transit/target/compiled/nyc_transit/analyses/days_before_precip_more_bike_trips.sql
