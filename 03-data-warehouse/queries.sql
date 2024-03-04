-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`
OPTIONS (
  format = "PARQUET",
  uris = ['gs://terraform-stavros-bucket/green/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned` AS
SELECT * FROM ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022;

/* --------- */
/* --------- */

-- Question 1: What is count of records for the 2022 Green Taxi Data?
SELECT count(1) FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`; --840402

/* --------- */
/* --------- */

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT DISTINCT PULocationID FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`;
SELECT DISTINCT PULocationID FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned`; --0 MB for the External Table and 6.41MB for the Materialized Table

/* --------- */
/* --------- */

-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(1) 
FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned` 
WHERE fare_amount = 0; --1,622

/* --------- */
/* --------- */

/* Question 4: What is the best strategy to make an optimized table in Big Query if your query will always 
order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)*/

-- Creating a partitioned and clustered table
CREATE OR REPLACE TABLE ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022;

/* --------- */
/* --------- */

/* Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to 
the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?*/

-- Using the non partitioned table
SELECT DISTINCT PULocationID 
FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned`
WHERE DATE_TRUNC(lpep_pickup_datetime, DAY) BETWEEN '2022-06-01' AND '2022-06-30' --12.82 MBs processed

-- Using the partitioned & clustered table
SELECT DISTINCT PULocationID 
FROM `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_partitioned_clustered`
WHERE DATE_TRUNC(lpep_pickup_datetime, DAY) BETWEEN '2022-06-01' AND '2022-06-30' --1.12 MBs processed




