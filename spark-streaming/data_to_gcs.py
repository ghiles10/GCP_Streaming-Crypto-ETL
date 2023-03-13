from pyspark.sql import SparkSession
from preprocess_stream_functions import read_kafka_streams, preprocess_stream
import os 
import logging_conf

logger = logging_conf.logger

# Récupération des variables d'environnement
ADDRESS = os.environ['KAFKA_ADDRESS'] 
DATA_PATH = os.environ['DATA_PATH']
CHECKPOINT_PATH = os.environ['CHECKPOINT_PATH']

# Création de la session Spark
logger.debug("Creating spark session for kafka stream")
spark = (SparkSession.builder
         .appName("finance")
         .master("yarn")
         .getOrCreate())

# Lecture du flux de données à partir de Kafka
logger.debug(f"Reading data from kafka with {ADDRESS} address") 
data_stream = read_kafka_streams(ADDRESS, spark) 

# Prétraitement du flux de données
logger.debug(f"Begin preprocess of data from kafka with {ADDRESS} address")
data_stream_read = preprocess_stream(spark, data_stream=data_stream)

# Écriture du flux de données dans un fichier CSV partitionné par année, mois, jour et heure
logger.debug(f"Writing data to {DATA_PATH} path & {CHECKPOINT_PATH} checkpoint path")
query = (data_stream_read
         .writeStream
         .outputMode("append")
         .format("csv")
         .option("header", "true")
         .partitionBy("year", "month", "day", "hour")
         .option("path", DATA_PATH)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(processingTime="10 seconds")
         .start())

# Attendre la fin de l'exécution de la requête
query.awaitTermination()
