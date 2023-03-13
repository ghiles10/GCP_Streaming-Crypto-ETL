#!bin/bash 

echo " !!!! export KAFKA_ADDRESS=IP.ADD.RE.SS"
echo "The Kafka Control Center should be available on port 9021"

cd ~/kafka && \
docker-compose build && \
docker-compose up -d

echo "Kafka is running" 
echo "extracting data from url's" 

python ~/extract/send_to_kafka.py 

