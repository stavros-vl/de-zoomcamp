Quick hack to load files directly to GCS, without Airflow. Downloads parquet files from https://d37ci6vzurychx.cloudfront.net/trip-data/ and uploads them to your Cloud Storage Account.

1. Install pre-reqs (more info in `web_to_gcs_green_2022.py` script)
2. Run: `python web_to_gcs_green_2022.py`