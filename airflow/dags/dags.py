import os 
from google.cloud import bigquery
from airflow import DAG
from schema import SCHEMA_FACT, SCHEMA_DIM_TIME, SCHEMA_DIM_STOCK
from query import QUERY_FACT
from airflow.operators.python import PythonOperator 
from tasks_template import create_insert_temp_table, create_biq_query_table, insert_job_dim_time, insert_job_fact

import datetime

PROJET_ID = os.environ.get('PROJECT_ID', "data-engineering-streaming") 
DATASET_ID= os.environ.get('DATASET_ID', "finance") 
TABLE_ID= os.environ.get('TABLE_ID', "temp_table") 
BUCKET = os.environ.get('BUCKET', "gs://kafka-finance-data")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/plugins/google_credentials.json") 

default_args = {
    'owner' : 'airflow'
} 

with DAG(
    dag_id = 'kafka-finance',
    default_args = default_args,
    description = 'Hourly data pipeline to generate dims and facts for streamify',
    schedule_interval="30 * * * *", 
    start_date=datetime.datetime.today() ,
    catchup=False,
    max_active_runs=1
    
) as dag: 
        
    client = bigquery.Client()

    
    create_insert_temp_table_big_query = PythonOperator( python_callable = create_insert_temp_table,
                                                  task_id = 'create_insert_temp_table',  
                                                    op_kwargs = { 'PROJET_ID': PROJET_ID,
                                                                    'DATASET_ID': DATASET_ID,
                                                                    'TABLE_ID': TABLE_ID ,
                                                                    'BUCKET': BUCKET, 
                                                                     'client' : client } ) 

    create_table_fact = PythonOperator( python_callable = create_biq_query_table, 
                                        task_id = "create_fact_table" , 
                                        op_kwargs = { 'PROJET_ID': PROJET_ID, 
                                                        'DATASET_ID': DATASET_ID,
                                                        'TABLE_ID': "fact",
                                                        'schema': SCHEMA_FACT,
                                                          'client' : client } 
                                                                        )

    create_dim_table_time = PythonOperator( python_callable = create_biq_query_table, 
                                        task_id = "create_dim_table_time" , 
                                        op_kwargs = { 'PROJET_ID': PROJET_ID, 
                                                        'DATASET_ID': DATASET_ID,
                                                        'TABLE_ID': "dim_time",
                                                        'schema': SCHEMA_DIM_TIME,
                                                          'client' : client } 
                                                                        )
 
    create_tem_table_stock_exchange_price = PythonOperator( python_callable = create_biq_query_table, 
                                        task_id = "create_dim_table_stock_exchange_price" , 
                                        op_kwargs = { 'PROJET_ID': PROJET_ID, 
                                                        'DATASET_ID': DATASET_ID,
                                                        'TABLE_ID': "dim_stock",
                                                        'schema': SCHEMA_DIM_STOCK,
                                                        'client' : client } 
                                                                        )


    insert_job_fact_table = PythonOperator( python_callable = insert_job_fact,
                                        task_id = "insert_data_fact_table" ,
                                        op_kwargs = { 'DATASET_ID': DATASET_ID,
                                                        'table_ref_id': "fact",
                                                        'query': QUERY_FACT,
                                                        'client': client } ) 
    


    create_insert_temp_table_big_query >> [create_table_fact, create_dim_table_time, create_tem_table_stock_exchange_price ] >> insert_job_fact_table
