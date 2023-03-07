import os 
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator 
from tasks_template import create_insert_temp_table, create_biq_query_table, insert_job 
import datetime

PROJET_ID = os.environ.get('PROJECT_ID') 
DATASET_ID= os.environ.get('DATASET_ID') 
TABLE_ID= os.environ.get('TABLE_ID') 
BUCKET = os.environ.get('BUCKET')


default_args = {
    'owner' : 'airflow'
} 

with DAG(
    dag_id = f'test',
    default_args = default_args,
    description = f'Hourly data pipeline to generate dims and facts for streamify',
    schedule_interval="30 * * * *", 
    start_date=datetime.datetime.today() ,
    catchup=False,
    max_active_runs=1
) as dag: 

    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("nom", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("prenom", "STRING", mode="REQUIRED"),
    ]   
    type = 'DAY'
    field = 'date'
    table = 'dim_test'

    create_tem_table = create_biq_query_table(PROJET_ID, DATASET_ID, TABLE_ID, schema, type, field, table)

    create_tem_table
