FROM mdillon/postgis

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gzip

RUN mkdir -p /data
RUN chmod -R 777 /data

COPY 01_initdb.sql docker-entrypoint-initdb.d/01_initdb.sql
COPY 02_prepare_pop_data.sh docker-entrypoint-initdb.d/02_prepare_pop_data.sh