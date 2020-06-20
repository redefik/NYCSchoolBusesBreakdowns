#!/bin/bash

docker stop influxdb
docker stop chronograf

docker rm influxdb
docker rm chronograf

docker network remove influxdb_chronograf_net
