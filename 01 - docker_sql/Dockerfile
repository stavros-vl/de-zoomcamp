FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest-data-to-postgres.py ingest-data-to-postgres.py

ENTRYPOINT [ "python", "ingest-data-to-postgres.py" ]