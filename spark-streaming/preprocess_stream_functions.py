from pyspark.sql.functions import expr, from_json, month, hour, dayofmonth, col, year, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging_conf   

logger = logging_conf.logger

def read_kafka_streams(address, spark, topic='finance'):
    
    """ lire un flux de données à partir d'un topic Kafka"""

    logger.debug("Reading data from kafka topic: {}".format(topic)) 

    # Définition du flux d'entrée
    data_stream = (spark.readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", address)
                   .option("subscribe", topic)
                   .option("startingOffsets", "earliest")
                   .load())
    return data_stream

def preprocess_stream(data_stream, topic="finance"):
    
    """Fonction pour prétraiter un flux de données à partir d'un topic Kafka"""

    # Définition du schéma des données JSON
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

    # Transformation de la colonne "value" de string à JSON et sélection des champs
    logger.debug("Transforming data from kafka topic: {}".format(topic)) 
    data_stream_json = (data_stream
                        .select(from_json(expr("CAST(value AS STRING)"), json_schema).alias("json_data"))
                        .selectExpr("json_data.time", "json_data.symbol", "json_data.buy", "json_data.sell", "json_data.changeRate",
                                    "json_data.changePrice", "json_data.high", "json_data.low", "json_data.vol", "json_data.volValue",
                                    "json_data.last", "json_data.averagePrice", "json_data.takerFeeRate", "json_data.makerFeeRate",
                                    "json_data.takerCoefficient", "json_data.makerCoefficient")
                       )

    # Transformation de la colonne "time" en timestamp et ajout des colonnes "year", "month", "day" et "hour"
    logger.debug(f"Adding year, month, day and hour columns to the data from kafka topic: {topic}")
    data_stream_json = (data_stream_json.withColumn("time", (col("time")/1000).cast("timestamp"))
                                         .withColumn("year", year("time")) 
                                         .withColumn("month", month("time"))
                                         .withColumn("day", dayofmonth("time")) 
                                         .withColumn("hour", hour("time"))  
                       )

    # Séparation de la colonne "symbol" pour ne garder que le nom de la crypto-monnaie
    data_stream_json = data_stream_json.withColumn("symbol", split(data_stream_json["symbol"], "-")[0]) 

    logger.debug("End of preprocessing from kafka topic: {}".format(topic)) 

    return data_stream
