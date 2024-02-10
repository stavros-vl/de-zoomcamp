import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import bigquery
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

output_file = "green_tripdata_2021.parquet"
dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def fetch_data_write_to_parquet(input_url, months, output_file):
    """
    Fetches data from URLs constructed based on input_url and months, loads it into a DataFrame,
    concatenates the DataFrames, and saves the concatenated DataFrame to Parquet files.

    Parameters:
    input_url (str): Base URL for the data files.
    months (list): List of months to fetch data for.
    output_file (str): Output file path where the Parquet files will be saved.

    Returns:
    None
    """
    assert isinstance(months, list)
    assert isinstance(input_url, str)
    
    dfs = []
    for month in months:
        url = f"{input_url}-{month}.csv.gz"  # Construct the URL for the CSV file
        df = pd.read_csv(url, compression='gzip')  # Read the CSV file directly from the URL into a DataFrame
        dfs.append(df) 
    
    # Concatenate the DataFrames
    concatenated_df = pd.concat(dfs, ignore_index=True)
    
    # Save the concatenated DataFrame to Parquet files
    concatenated_df.to_parquet(output_file)


def transform_df(df):  
    """
    Transform the input DataFrame by removing rows where trip distance or passenger count is equal to zero,
    converting 'lpep_pickup_datetime' to 'lpep_pickup_date', renaming columns to snake case,
    and performing assertions on the transformed DataFrame.

    Parameters:
    df (pd.DataFrame): Input DataFrame containing raw trip data.

    Returns:
    pd.DataFrame: Transformed DataFrame.
    
    Raises:
    AssertionError: If any of the following conditions are not met after transformation:
      - 'vendor_id' column contains only valid values.
      - All values in the 'passenger_count' column are greater than 0.
      - All values in the 'trip_distance' column are greater than 0.
    """  
    df = df.copy()

    condition = (df['trip_distance'].fillna(0) != 0) & (df['passenger_count'].fillna(0) != 0)
    df = df[condition]
    
    df['lpep_pickup_date'] = pd.to_datetime(df['lpep_pickup_datetime'])
    
    df.columns = (df.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             ) 
    
    assert df['vendor_id'].isin(df['vendor_id'].unique()).all(), "vendor_id contains invalid values"
    assert (df['passenger_count'] > 0).all(), "passenger_count contains values less than or equal to 0"
    assert (df['trip_distance'] > 0).all(), "trip_distance contains values less than or equal to 0"
    
    return df


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    # Read the Parquet file to inspect its contents
    parquet_data = pd.read_parquet(local_file)
    print("Parquet file contents:")
    print(parquet_data)

    # Print the schema of the Parquet file
    schema = pq.read_schema(local_file)
    print("Parquet file schema:")
    print(schema)

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="green_taxi_etl_dag",
    schedule_interval='0 5 * * *',  # Run daily at 5AM UTC
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    # Define the task that executes the load_df function
    fetch_and_write_to_parquet = PythonOperator(
        task_id='fetch_data_write_to_parquet',
        python_callable=fetch_data_write_to_parquet,
        op_args=[dataset_url, ['10', '11', '12'], output_file],
        dag=dag
    )