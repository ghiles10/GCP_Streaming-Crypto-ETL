from pyspark.sql import SparkSession
# Create a SparkSession 

def get_spark_session(app_name = 'finance', master="yarn"):
    
    spark = ( SparkSession
                .builder
                .appName(app_name)
                .master(master=master)
                .getOrCreate() ) 

    return spark  

spark = get_spark_session()

data_stream = ( spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka_address:kafka_port").option("subscribe", "finance")
    .option("startingOffsets", "earliest").load() )

# Path: spark-streaming/data_to_gcs.py 
data_stream.writeStream.format('parquet').option('path', 'gs://kafka-finance-data/data').option('checkpointLocation', 'gs://kafka-finance-data').start()

