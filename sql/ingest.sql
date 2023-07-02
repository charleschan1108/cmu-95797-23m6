-- SET number of threads to 4 to avoid out-of-memory error
SET threads TO 4;

-- Table 1: Bike_data 
-- Create table 'bike_data' and ingest bike data into it
-- use read_csv_auto to autodetect data schema
-- use union_by_name as the data have 2 set of schemas
-- increase sample size to 300k for schema detection 

.print 'Ingesting bike_data..'
CREATE TABLE bike_data AS SELECT * FROM read_csv_auto('../data/bike/*-citibike-tripdata.csv.gz', sample_size = 300000, union_by_name=True);

-- Table 2: central_park_weather
.print 'Ingesting central_park_weather..'
CREATE TABLE central_park_weather AS SELECT * FROM read_csv_auto("../data/central_park_weather.csv");

-- Table 3: daily_citi_bike_trip_counts_and_weather
.print 'Ingesting daily_citi_bike_trip_counts_and_weather..'
CREATE TABLE daily_citi_bike_trip_counts_and_weather AS SELECT * FROM read_csv_auto("../data/daily_citi_bike_trip_counts_and_weather.csv");

-- Table 4: fhv_bases
.print 'Ingesting fhv_bases..'
CREATE TABLE fhv_bases AS SELECT * FROM read_csv_auto("../data/fhv_bases.csv");

-- Talbe 5: fhv_tripdata
.print 'Ingesting fhv_tripdata..'
CREATE TABLE fhv_tripdata AS SELECT * FROM "../data/taxi/fhv_tripdata_*.parquet";

-- Talbe 6: fhvhv_tripdata
.print 'Ingesting fhvhv_tripdata..'
CREATE TABLE fhvhv_tripdata AS SELECT * FROM "../data/taxi/fhvhv_tripdata_*.parquet";

-- Talbe 7: green_tripdata
.print 'Ingesting green_tripdata..'
CREATE TABLE green_tripdata AS SELECT * FROM "../data/taxi/green_tripdata_*.parquet";

-- Talbe 8: yellow_tripdata
.print 'Ingesting yellow_tripdata..'
CREATE TABLE yellow_tripdata AS SELECT * FROM "../data/taxi/yellow_tripdata_*.parquet";

