from pyspark.sql import SparkSession
# Create a SparkSession 

def get_spark_session(app_name = 'finance'):
    
    spark = ( SparkSession
                .builder
                .appName(app_name)
                .getOrCreate() ) 

    return spark  

spark = get_spark_session()

data_stream = ( spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "finance")
    .option("startingOffsets", "earliest").load() )

# Path: spark-streaming/data_to_gcs.py 
( data_stream.writeStream.format('csv')
 .option('path', 'gs://kafka-finance-data/data')
 .trigger(processingTime="10 seconds" )
 .outputMode('append')  
 .option('checkpointLocation', 'gs://kafka-finance-data/checks').start() ) 

