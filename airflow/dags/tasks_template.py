from google.cloud import bigquery
import os 
import datetime

client = bigquery.Client() 


PROJET_ID = os.environ.get('PROJECT_ID') 
DATASET_ID= os.environ.get('DATASET_ID') 
TABLE_ID= os.environ.get('TABLE_ID') 

# Construct a BigQuery client object.
client = bigquery.Client()

table_id = f"{PROJET_ID}.{DATASET_ID}.{TABLE_ID}"
print(table_id, '----------------------') 

job_config = bigquery.LoadJobConfig(

    autodetect=True,
    skip_leading_rows=1,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="time"  # Name of the column to use for partitioning.
    ),
)
uri = "gs://kafka-finance-data/data/year=2023/month=3/day=7/*.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Wait for the job to complete.

table = client.get_table(table_id)
print(f"Loaded {table} rows to table {table_id}")

table.expires = datetime.datetime.now() + datetime.timedelta(days=1)
client.update_table(table, ['expires'])