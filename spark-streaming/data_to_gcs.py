from pyspark.sql import SparkSession
from preprocess_stream_functions import read_kafka_streams, preprocess_stream
import os 
import logging_conf

logger = logging_conf.logger

ADRESS = os.environ['KAFKA_ADRESS'] 
DATA_PATH = os.environ['DATA_PATH']
CHECKPOINT_PATH = os.environ['CHECKPOINT_PATH']

logger.debug("Creating spark session for kafka stream")

spark = (SparkSession.builder
            .appName("finance")
            .master("yarn")
            .getOrCreate()
            )

logger.debug(f"Reading data from kafka with {ADRESS} adress") 
data_stream = read_kafka_streams(ADRESS, spark) 

logger.debug(f" begin preprocess of data from kafka with {ADRESS} adress")
data_stream_read = preprocess_stream(spark, data_stream= data_stream )

logger.debug(f"Writing data to {DATA_PATH} path & {CHECKPOINT_PATH} checkpoint path")

query = (data_stream_read
         .writeStream
         .outputMode("append")
         .format("csv")
         .option("header", "true")
         .partitionBy("year" , "month", "day", "hour")
         .option("path", DATA_PATH )
         .option("checkpointLocation", CHECKPOINT_PATH )
         .trigger(processingTime="50 seconds")
         .start() )

query.awaitTermination()
