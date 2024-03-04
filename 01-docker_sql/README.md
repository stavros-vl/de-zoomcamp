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

### Convert jupyter notebook .ipynb file to python .py script

Run this command

```
jupyter nbconvert --to=script <name_of_the_destination_file>.ipynb
```

### Using argparse to parse command line arguments

We are using `argparse` standard library which helps us to create parse command line arguments instead of `sys.argv`.
That way we can have named arguments like user, password, host, database name, etc.

https://docs.python.org/3/library/argparse.html


### Dropping table and Running the script

To drop the tables, run the command `DROP TABLE yellow_taxi_data`

Now, the database table is empty, and running commands like, SELECT COUNT(1) FROM yellow_taxi_data will return nothing.

Running this locally now should start the ingestion script and make it populate the table in postgres again.


```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest-data-to-postgres.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```


This command is not safe as the we are passing the password as string and it saved in the history of the command line. Running the `history` command from shell will result in seeing whatever command we have inserted.

We are declaring URL variable and later accessing the variable with `${URL}` in the terminal.

To see error code from the terminal `echo $?`

If we now refresh the database from pgAdmin, we will be able to run `SELECT COUNT(1) FROM yellow_taxi_trips` and see it worked or not.

*BTW we have changed the table name from “yellow_taxi_data” to “yellow_taxi_trips”.*


### Dockerizing Ingestion Script


First we build the docker image by running this command

```
docker build -t taxi_ingest:v001 .
```

Then we run the dockerized script with the below command. *Note that the command is similar to the one in the section above, but now we run it through docker.*

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database2 \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```

### Why we need Docker Compose

https://docs.docker.com/compose/

Previously we have run a -

- In a network
    - run postgres
    - run pgAdmin

Instead of having all this messy configuration we can build a `YAML` file that describes all the configuration and connects the two databases with on network.

Docker Compose allows putting configuration of multiple containers in to one file. Instead of  writing all these commands we will just create everything in one fell sweep with Docker Compose. Docker Compose is a convenient way to run multiple related services with just one config file.

Run `docker-compose` if you have installed docker desktop. If not follow the instructions on how to install it.

#### Commands

Run the docker-compose file
```
docker-compose up
```

Run in detached mode:

```
docker-compose up -d
```
Shut down

```
docker-compose down
```
Note: to make pgAdmin configuration persistent, create a folder data_pgadmin. Change its permission via

```
sudo chown 5050:5050 data_pgadmin
```

and mount it to the `/var/lib/pgadmin` folder:

```
services:
  pgadmin:
    image: dpage/pgadmin4
    volumes:
      - ./data_pgadmin:/var/lib/pgadmin
    ...
```


