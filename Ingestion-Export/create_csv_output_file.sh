#!/bin/bash
# Before execution, move this script to Kafka installation directory

echo "Creating output files..."
touch avgDelaysByCounty24h.csv
touch avgDelaysByCounty7d.csv
touch avgDelaysByCounty30d.csv
touch breakdownReasonsByTimeSlot24h.csv
touch breakdownReasonsByTimeSlot7d.csv
touch topCompaniesByDisservice24h.csv
touch topCompaniesByDisservice7d.csv
echo "Launch Kafka consumers..."
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avgDelaysByCounty24h > avgDelaysByCounty24h.csv &
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avgDelaysByCounty7d > avgDelaysByCounty7d.csv &
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avgDelaysByCounty30d > avgDelaysByCounty30d.csv &
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic breakdownReasonsByTimeSlot24h > breakdownReasonsByTimeSlot24h.csv &
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic breakdownReasonsByTimeSlot7d > breakdownReasonsByTimeSlot7d.csv &
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topCompaniesByDisservice24h > topCompaniesByDisservice24h.csv &
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topCompaniesByDisservice7d > topCompaniesByDisservice7d.csv &
echo "Kafka consumers launched"
