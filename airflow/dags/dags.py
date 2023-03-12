import os 
from google.cloud import bigquery
from airflow import DAG
import datetime
from schema import SCHEMA_FACT, SCHEMA_DIM_TIME, SCHEMA_DIM_STOCK
from query import QUERY_FACT, QUERY_DIM_TIME, QUERY_DIM_STOCK
from airflow.operators.python import PythonOperator 
from tasks_template import (create_insert_temp_table, 
                            create_biq_query_table, insert_job_dim_time, insert_job_fact, insert_job_dim_stock,drop_temp_table ) 
from airflow.operators.dummy_operator import DummyOperator


PROJET_ID = os.environ.get('PROJECT_ID')
DATASET_ID= os.environ.get('DATASET_ID') 
TABLE_ID= os.environ.get('TABLE_ID') 
BUCKET = os.environ.get('BUCKET')
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") 

default_args = {
    'owner' : 'airflow' , 
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'queue': 'default',
    'pool': 'default_pool',
    'sla': datetime.timedelta(hours=1)
} 

with DAG(
    dag_id = 'kafka-finance',
    default_args = default_args,
    description = 'data pipeline to generate dims and facts for finance crypto data',
    schedule_interval="30 * * * *", 
    start_date=datetime.datetime.today() ,
    catchup=False,
    max_active_runs=3
    
) as dag: 
        
    client = bigquery.Client()
    
    created_tables = DummyOperator(task_id='created_tables')

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
 
    create_table_stock_exchange_price = PythonOperator( python_callable = create_biq_query_table, 
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

    insert_data_dim_time = PythonOperator( python_callable = insert_job_dim_time, 
                                    task_id = "insert_data_dim_time_table" ,
                                    op_kwargs = { 'DATASET_ID': DATASET_ID,
                                                    'table_ref_id': "dim_time",
                                                    'query': QUERY_DIM_TIME,
                                                    'client': client } ) 
    
    insert_job_dim_stock_table = PythonOperator( python_callable = insert_job_dim_stock,
                                        task_id = "insert_data_dim_stock_table" ,
                                        op_kwargs = { 'DATASET_ID': DATASET_ID,
                                                        'table_ref_id': "dim_stock",
                                                        'query': QUERY_DIM_STOCK,
                                                        'client': client } ) 
    
    drop_temporary_table = PythonOperator( python_callable = drop_temp_table, 
                                    task_id = "drop_temporary_table" ,
                                    op_kwargs = { 'DATASET_ID': DATASET_ID,
                                                    'TABLE_ID': TABLE_ID,
                                                    'client': client } ) 


    [create_insert_temp_table_big_query ,create_table_fact, create_dim_table_time, create_table_stock_exchange_price] >>created_tables>> [insert_job_fact_table ,insert_data_dim_time, insert_job_dim_stock_table] >> drop_temporary_table
    


