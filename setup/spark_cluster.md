# Setup Spark Cluster

The Spark Streaming application runs on a Google Cloud Platform (GCP) DataProc cluster and processes the data retrieved from Kafka through port 9092.
By utilizing the capabilities of the DataProc cluster, the application efficiently handles large-scale data streams and performs necessary transformations and computations.
This setup enables real-time data processing, ensuring that the system remains up-to-date and responsive to ever-changing data sources.

This operation will preprocess data from kafka producer with Spark Streaming and send it to partitioned CSV files in Google Cloud Storage

- Establish SSH connection to the master node 

```ini
git clone https://github.com/ghiles10/GCP_Streaming-Crypto-ETL.git && \
cd CP_Streaming-Crypto-ETL/spark_streaming 
```

- Environnement variables 

```ini
export KAFKA_ADDRESS=ADRESS:9092
export CHEKS_PATH=gs://path/to/chekpoints 
export DATA_PATH=gs://path/to/data 
```

- Start reading messages 

```ini
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
data_to_gcs.py
```

