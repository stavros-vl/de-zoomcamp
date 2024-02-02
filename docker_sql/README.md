### Docker and SQL
Notes I used for preparing the videos: link

### Commands
All the commands from the video

Downloading the data

```
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```

>Note: now the CSV data is stored in the csv_backup folder, not trip+date like previously


### Running Postgres and pgAdmin together

Create a network

```
docker network create pg-network
```

Run Postgres (change the path) [Linux/MacOS]

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database2 \
  postgres:13
```

Run pgAdmin

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```
