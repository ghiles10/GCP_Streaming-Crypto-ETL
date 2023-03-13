from google.cloud import bigquery

SCHEMA_FACT = [
    bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("volvalue", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE")
]   


SCHEMA_DIM_TIME = [
    bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("year", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("month", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("day", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("hour", "INT64", mode="NULLABLE")
]   

SCHEMA_DIM_STOCK = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("buy", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("sell", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("changerate", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("changeprice", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("vol", "FLOAT64", mode="NULLABLE")

]   

