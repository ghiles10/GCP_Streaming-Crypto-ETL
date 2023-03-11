import os 
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator 
from tasks_template import create_insert_temp_table, create_biq_query_table, insert_job 
import datetime

PROJET_ID = os.environ.get('PROJECT_ID', "data-engineering-streaming") 
DATASET_ID= os.environ.get('DATASET_ID', "finance") 
TABLE_ID= os.environ.get('TABLE_ID', "temp-table") 
BUCKET = os.environ.get('BUCKET', "gs://kafka-finance-data")


default_args = {
    'owner' : 'airflow'
} 

with DAG(
    dag_id = 'kafka-finance',
    default_args = default_args,
    description = f'Hourly data pipeline to generate dims and facts for streamify',
    schedule_interval="30 * * * *", 
    start_date=datetime.datetime.today() ,
    catchup=False,
    max_active_runs=1
    
) as dag: 
    
    ###### 
    create_insert_temp_table = PythonOperator( python_callable = create_insert_temp_table,
                                                  task_id = 'create_insert_temp_table',  
                                                    op_kwargs = { 'PROJET_ID': PROJET_ID,
                                                                    'DATASET_ID': DATASET_ID,
                                                                    'TABLE_ID': TABLE_ID ,
                                                                    'BUCKET': BUCKET } ) 
    #########
    schema_fact = [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("volValue", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE")
    ]   
    create_table_fact = PythonOperator( python_callable = create_biq_query_table, 
                                        task_id = "create_fact_table" , 
                                        op_kwargs = { 'PROJET_ID': PROJET_ID, 
                                                        'DATASET_ID': DATASET_ID,
                                                        'TABLE_ID': "fact",
                                                        'schema': schema_fact } 
                                                                        )
    #########	
    schema_dim_time = [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("year", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("month", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("day", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("hour", "INT64", mode="REQUIRED")
    ]   
    create_dim_table_time = PythonOperator( python_callable = create_biq_query_table, 
                                        task_id = "create_dim_table_time" , 
                                        op_kwargs = { 'PROJET_ID': PROJET_ID, 
                                                        'DATASET_ID': DATASET_ID,
                                                        'TABLE_ID': "dim-time",
                                                        'schema': schema_dim_time } 
                                                                        )
    #########
    schema_dim_stock = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("buy", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("sell", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("changeRate", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("changePrice", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("vol", "FLOAT64", mode="NULLABLE")

    ]   
    create_tem_table_stock_exchange_price = PythonOperator( python_callable = create_biq_query_table, 
                                        task_id = "create_dim_table_stock_exchange_price" , 
                                        op_kwargs = { 'PROJET_ID': PROJET_ID, 
                                                        'DATASET_ID': DATASET_ID,
                                                        'TABLE_ID': "dim-stock",
                                                        'schema': schema_dim_stock } 
                                                                        )


    create_insert_temp_table >> [create_table_fact, create_dim_table_time, create_tem_table_stock_exchange_price ]
