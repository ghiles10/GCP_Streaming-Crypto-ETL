from pyspark.sql.functions import expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql import SparkSession
import time

def get_spark_session(app_name='finance'):
    spark = (SparkSession.builder
             .appName(app_name)
             .getOrCreate())
    return spark

spark = get_spark_session()

# Define the schema of the JSON data
json_schema = StructType([
  StructField("time", LongType()),
  StructField("symbol", StringType()),
  StructField("buy", StringType()),
  StructField("sell", StringType()),
  StructField("changeRate", StringType()),
  StructField("changePrice", StringType()),
  StructField("high", StringType()),
  StructField("low", StringType()),
  StructField("vol", StringType()),
  StructField("volValue", StringType()),
  StructField("last", StringType()),
  StructField("averagePrice", StringType()),
  StructField("takerFeeRate", StringType()),
  StructField("makerFeeRate", StringType()),
  StructField("takerCoefficient", StringType()),
  StructField("makerCoefficient", StringType())
])

# Define the input data stream
data_stream = (spark.readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "10.154.0.19:9092")
               .option("subscribe", "finance")
               .option("startingOffsets", "earliest")
               .load())

# Transform the value column from string to JSON and select the fields
data_stream_json = (data_stream
                    .select(from_json(expr("CAST(value AS STRING)"), json_schema).alias("json_data"))
                    .selectExpr("json_data.time", "json_data.symbol", "json_data.buy", "json_data.sell", "json_data.changeRate",
                                "json_data.changePrice", "json_data.high", "json_data.low", "json_data.vol", "json_data.volValue",
                                "json_data.last", "json_data.averagePrice", "json_data.takerFeeRate", "json_data.makerFeeRate",
                                "json_data.takerCoefficient", "json_data.makerCoefficient"))

# Staet the query to write the output to a memoty table 
query = (data_stream_json
         .writeStream
         .outputMode("append")
         .format("memory")
         .queryName("test")
         .trigger(processingTime="3 seconds")
         .start())

# Print the output DataFrame every 3 seconfs 
while True : 
  spark.sql("SELECT * FROM test").show(truncate=False)
  time.sleep(3)