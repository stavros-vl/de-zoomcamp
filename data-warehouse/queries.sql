-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022`
OPTIONS (
  format = "PARQUET",
  uris = ['gs://terraform-stavros-bucket/green/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022_non_partitioned` AS
SELECT * FROM ny-rides-stavros-zoomcamp.trips_data_all.green_tripdata_2022;