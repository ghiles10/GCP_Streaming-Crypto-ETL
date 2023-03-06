from pyspark.sql import SparkSession

def get_spark_session(app_name='finance'):
    spark = (SparkSession.builder
             .appName(app_name)
             .getOrCreate())
    return spark

spark = get_spark_session()

data_stream = (spark.readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "10.154.0.19:9092")
               .option("subscribe", "finance")
               .option("startingOffsets", "earliest")
               .load())

query = (data_stream
         .writeStream
         .outputMode("append")
         .format("csv")
         .option("path", "gs://kafka-finance-data/data//")
         .option("checkpointLocation", "gs://kafka-finance-data/checks/")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()
