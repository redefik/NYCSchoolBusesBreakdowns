#!/bin/bash

# This script creates the Kafka topics used to:
# 1) Ingest data from the stream simulator into the Flink application
# 2) Export output produced by Flink
# NOTE: Before the execution, copy this script to the Kafka installation directory
echo "Creating input topics..."
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic breakdowns1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic breakdowns2
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic breakdowns3
echo "Creating output topics..."
echo "First query..."
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic avgDelaysByCounty24h
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic avgDelaysByCounty7d
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic avgDelaysByCounty30d
echo "Second query..."
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic breakdownReasonsByTimeSlot24h
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic breakdownReasonsByTimeSlot7d
echo "Third query..."
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic topCompaniesByDisservice24h
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic topCompaniesByDisservice7d





