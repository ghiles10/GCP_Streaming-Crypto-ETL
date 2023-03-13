#!bin/bash 

echo " Launching Spark Streaming Job" 
echo "!!!! export DATA_PATH=gs://bucket-name/path/to/data" 
echo "!!!! export CHEKPOINT_PATH=gs://bucket-name/path/to/checkpoint"
echo "!!!! export KAFKA_ADDRESS=IP.ADD.RE.SS:9092" 

spark-submit \
  --deploy-mode cluster \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
spark-streaming/data_to_gcs.py 