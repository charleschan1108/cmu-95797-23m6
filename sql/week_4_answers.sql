-- print scripts
.echo ON

-- show all results
.maxrows 100

-- Q1 bike_trips_and_duration_by_weekday.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/bike_trips_and_duration_by_weekday.sql

-- Q2 taxi_trips_ending_at_airport.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/taxi_trips_ending_at_airport.sql

-- Q3 inter_borough_taxi_trips_by_weekday.sql
.read ../nyc_transit/target/compiled/nyc_transit/analyses/inter_borough_taxi_trips_by_weekday.sql

