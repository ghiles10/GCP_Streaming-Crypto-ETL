from pyspark.sql.functions import expr, from_json, month, hour, dayofmonth, col, year, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import time
import logging_conf   

logger = logging_conf.logger

def read_kafka_streams(adress,spark, topic = 'finance'): 

  # Define the input data stream
  logger.debug("Reading data from kafka topic: {}".format(topic)) 
               
  data_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", adress)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load())
  
  return data_stream

def preprocess_stream(spark, data_stream,  topic = "finance", output_mode="append", trigger="3 seconds"): 

  """ this function allows to read data from a kafka topic and transform it"""
   
  # Define the schema of the JSON data
  json_schema = StructType([
    StructField("time", LongType()),
    StructField("symbol", StringType()),
    StructField("buy", DoubleType()),
    StructField("sell", DoubleType()),
    StructField("changeRate", DoubleType()),
    StructField("changePrice", DoubleType()),
    StructField("high", DoubleType()), 
    StructField("low", DoubleType()),
    StructField("vol", DoubleType()),
    StructField("volValue", DoubleType()),
    StructField("last", DoubleType()),
    StructField("averagePrice", DoubleType()),
    StructField("takerFeeRate", DoubleType()),
    StructField("makerFeeRate", DoubleType()),
    StructField("takerCoefficient", DoubleType()),
    StructField("makerCoefficient", DoubleType())
  ])

  
  # Transform the value column from string to JSON and select the fields
  logger.debug("Transforming data from kafka topic: {}".format(topic)) 

  data_stream_json = (data_stream
                      .select(from_json(expr("CAST(value AS STRING)"), json_schema).alias("json_data"))
                      .selectExpr("json_data.time", "json_data.symbol", "json_data.buy", "json_data.sell", "json_data.changeRate",
                                  "json_data.changePrice", "json_data.high", "json_data.low", "json_data.vol", "json_data.volValue",
                                  "json_data.last", "json_data.averagePrice", "json_data.takerFeeRate", "json_data.makerFeeRate",
                                  "json_data.takerCoefficient", "json_data.makerCoefficient")
                                  )

  # transofmre the time column to timestamp and add year, month, day and hour columns
  logger.debug(f"Adding year, month, day and hour columns to the data from kafka topic: {topic}")

  data_stream_json=  ( data_stream_json.withColumn("time", (col("time")/1000).cast("timestamp") )
                                    .withColumn("year", year("time")) 
                                    .withColumn("month", month("time"))
                                    .withColumn("day", dayofmonth("time")) 
                                    .withColumn("hour", hour("time"))  
                                            )                                

  data_stream_json =  data_stream_json.withColumn("symbol", split(data_stream_json["symbol"], "-")[0]) 
  logger.debug("end of preprocessing from kafka topic: {}".format(topic) ) 

  return data_stream_json

  # # Staet the query to write the output to a memoty table 
  # query = (data_stream_json
  #         .writeStream
  #         .outputMode(output_mode)
  #         .format("memory")
  #         .queryName("test")
  #         .trigger(processingTime=trigger)
  #         .start())

  # # Print the output DataFrame every 3 seconfs 
  # while True : 
  #   spark.sql("SELECT * FROM test").show(truncate=False)
  #   time.sleep(3)                           
