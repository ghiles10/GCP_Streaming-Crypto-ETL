from google.cloud import bigquery
import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

def create_insert_temp_table(PROJET_ID, DATASET_ID, TABLE_ID, BUCKET)   : 

    """ create a temp table to insert data from GCS to BigQuery"""
    
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = f"{PROJET_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(

        autodetect=True,
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="time"  # Name of the column to use for partitioning.
        ),
    )

    # define date 
    now = datetime.datetime.now() 
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    
    uri = f"{BUCKET}/data/year={year}/month={month}/day={day}/*.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)

    table.expires = datetime.datetime.now() + datetime.timedelta( minutes=30 )
    client.update_table(table, ['expires'])  

def create_biq_query_table(PROJET_ID, DATASET_ID, TABLE_ID, schema, type, field, table)   :

    """  Create an empty table in Bigquery """

    task = BigQueryCreateEmptyTableOperator(

        task_id = f'{datetime.datetime.now()}_create_empty_table',
        project_id = PROJET_ID,
        dataset_id = DATASET_ID,
        table_id = TABLE_ID + "-" +table ,
        schema_fields = schema,
        time_partitioning = {
            'type': type,
            'field': field
            },
        exists_ok = True
    )

    return task

# schema = [
#     bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
#     bigquery.SchemaField("nom", "STRING", mode="REQUIRED"),
#     bigquery.SchemaField("prenom", "STRING", mode="REQUIRED"),
# ]    
# type = 'DAY'
# field = 'date'
# table = 'dim_test'

def insert_job(DATASET_ID, PROJET_ID, query, timeout):

    """ Insert data from a query to Bigquery """ 

    task = BigQueryInsertJobOperator(

        task_id = f'{datetime.datetime.now()}_execute_insert_query',
        configuration = {
        
            'query': {
                'query': query,
                'useLegacySql': False
            },
            
            'timeoutMs' : timeout,
            'defaultDataset' : {
                'datasetId': DATASET_ID,
                'projectId': PROJET_ID
                }
            }
    )

    return task
