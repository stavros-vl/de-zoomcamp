# Data Warehouse and BigQuery

See: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse

The aim here is to get practice with BigQuery and different types of tables as well as performance and cost considerations. Functionalities such as partitioning and clustering will be explored.

### Note 

For this exercise we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
To make things faster/simpler we will not use an orchestrator to load the data but use a script instead `web_to_gcs_green_2022.py`.
This script will download the parquet files from the link and load into a bucket in GCS.


### SETUP
Create an external table using the Green Taxi Trip Records Data for 2022.
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

### Questions to be answered

**Question 1:** *What is count of records for the 2022 Green Taxi Data??*

**Question 2:** *Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.*
*What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?*

**Question 3:** *How many records have a fare_amount of 0?*

**Question 4:** *What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)*

**Question 5:** *Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)*

*Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?*

**Question 6:** *Where is the data stored in the External Table you created?*

**Question 7:** *It is best practice in Big Query to always cluster your data:*
