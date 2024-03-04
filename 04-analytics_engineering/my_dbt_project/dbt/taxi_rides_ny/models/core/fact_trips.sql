{{
    config(
        materialized='table'
    )
}}

with green_tripdata AS (
    SELECT *, 
        'Green' AS service_type
    FROM {{ ref('stg_green_tripdata_2019') }}

    UNION ALL

    SELECT *, 
        'Green' AS service_type
    FROM {{ ref('stg_green_tripdata_2020') }}
), 
yellow_tripdata AS (
    SELECT *, 
        'Yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata_2019') }}

    UNION ALL

    SELECT *, 
        'Yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata_2020') }}
), 
trips_unioned AS (
    SELECT * FROM green_tripdata

    UNION ALL 

    SELECT * FROM yellow_tripdata
), 
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)
SELECT 
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough AS pickup_borough, 
    pickup_zone.zone AS pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough AS dropoff_borough, 
    dropoff_zone.zone AS dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.pAS, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
FROM trips_unioned
INNER JOIN dim_zones AS pickup_zone
ON trips_unioned.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones AS dropoff_zone
ON trips_unioned.dropoff_locationid = dropoff_zone.locationid