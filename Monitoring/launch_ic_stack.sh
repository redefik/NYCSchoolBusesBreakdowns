#!/bin/bash

# This script launches a Docker network encapsulating two components of the
# InfluxDB TICK stack, namely:
# Influx DB (I)
# Chronograf (C)

docker network create influxdb_chronograf_net

# InfluxDB setup
docker run -d --name influxdb --network=influxdb_chronograf_net -p 8086:8086 \
           -v "$PWD/influxdb_data:/var/lib/influxdb" influxdb
         
# Chronograf setup
docker run -d --name chronograf --network=influxdb_chronograf_net -p 8888:8888 \
           -v "$PWD/chronograf_data:/var/lib/chronograf" chronograf \
           --influxdb-url=http://influxdb:8086
