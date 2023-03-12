from google.cloud import bigquery

SCHEMA_FACT = [
    bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("volvalue", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("high", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("low", "STRING", mode="NULLABLE")
]   


SCHEMA_DIM_TIME = [
    bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("year", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("month", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("day", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("hour", "INT64", mode="REQUIRED")
]   

SCHEMA_DIM_STOCK = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("buy", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sell", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("changerate", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("changeprice", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vol", "STRING", mode="NULLABLE")

]   

