from pyspark.sql import SparkSession
from preprocess_stream_functions import read_kafka_streams, preprocess_stream
import os 

ADRESS = os.environ['KAFKA_ADRESS'] 
DATA_PATH = os.environ['DATA_PATH']
CHECKPOINT_PATH = os.environ['CHECKPOINT_PATH']

spark = (SparkSession.builder
            .appName("finance")
            .master("yarn")
            .getOrCreate()
            )

data_stream = read_kafka_streams(ADRESS, spark) 
data_stream_read = preprocess_stream(spark, data_stream= data_stream )

query = (data_stream_read
         .writeStream
         .outputMode("append")
         .format("csv")
         .option("header", "true")
         .partitionBy("year" , "month", "day", "hour")
         .option("path", DATA_PATH )
         .option("checkpointLocation", CHECKPOINT_PATH )
         .trigger(processingTime="10 seconds")
         .start() )

query.awaitTermination()
