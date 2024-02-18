-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`
OPTIONS (
  format = "PARQUET",
  uris = ['gs://terraform-stavros-bucket/green/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned` AS
SELECT * FROM ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022;

-- Question 1: What is count of records for the 2022 Green Taxi Data?
SELECT count(1) FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`; --840402

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT DISTINCT PULocationID FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`;
SELECT DISTINCT PULocationID FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned`; --0 MB for the External Table and 6.41MB for the Materialized Table

-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(1) 
FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned` 
WHERE fare_amount = 0; --1,622
