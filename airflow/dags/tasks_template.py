from google.cloud import bigquery
import datetime


def create_insert_temp_table(PROJET_ID, DATASET_ID, TABLE_ID, BUCKET, client):
    """create a temp table to insert data from GCS to BigQuery"""

    table_id = f"{PROJET_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="time",  # Name of the column to use for partitioning.
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

    table.expires = datetime.datetime.now() + datetime.timedelta(minutes=30)
    client.update_table(table, ["expires"])


def create_biq_query_table(PROJET_ID, DATASET_ID, TABLE_ID, schema, client):
    """create table in BigQuery"""

    table_id = f"{PROJET_ID}.{DATASET_ID}.{TABLE_ID}"

    # Créer une instance de la classe Table
    table = bigquery.Table(table_id, schema=schema)

    # Créez la table dans BigQuery
    table = client.create_table(table, exists_ok=True)


def insert_job_fact(DATASET_ID, table_ref_id, query, client):
    """retreive data from a query ( temp table) and insert to fact and dim tables"""

    query_job = client.query(query)
    results = query_job.result()

    table_ref = client.dataset(DATASET_ID).table(table_ref_id)
    table = client.get_table(table_ref)

    rows_to_insert = [
        (str(row.time), row.symbol, row.volvalue, row.high, row.low) for row in results
    ]
    if rows_to_insert:
        client.insert_rows(table, rows_to_insert)
    else:
        raise ValueError(
            f"No rows to insert, len of rows_to_insert is {len(rows_to_insert)}"
        )


def insert_job_dim_time(DATASET_ID, table_ref_id, query, client):
    """retreive data from a query ( temp table) and insert to fact and dim time table"""

    query_job = client.query(query)
    results = query_job.result()

    table_ref = client.dataset(DATASET_ID).table(table_ref_id)
    table = client.get_table(table_ref)

    rows_to_insert = [
        (str(row.time), str(row.year), row.month, row.day, row.hour) for row in results
    ]
    if rows_to_insert:
        client.insert_rows(table, rows_to_insert)
    else:
        raise ValueError(
            f"No rows to insert, len of rows_to_insert is {len(rows_to_insert)}"
        )


def insert_job_dim_stock(DATASET_ID, table_ref_id, query, client):
    """retreive data from a query ( temp table) and insert to fact and dim stock table"""

    query_job = client.query(query)
    results = query_job.result()

    table_ref = client.dataset(DATASET_ID).table(table_ref_id)
    table = client.get_table(table_ref)

    rows_to_insert = [
        (row.symbol, row.buy, row.sell, row.changerate, row.changeprice, row.vol)
        for row in results
    ]
    if rows_to_insert:
        client.insert_rows(table, rows_to_insert)
    else:
        raise ValueError(
            f"No rows to insert, len of rows_to_insert is {len(rows_to_insert)}"
        )


def drop_temp_table(DATASET_ID, TABLE_ID, client):
    """drop temp table"""

    # Récupération de l'objet Table à supprimer
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = client.get_table(table_ref)

    # Suppression de la table
    client.delete_table(table)
