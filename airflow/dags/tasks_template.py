from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator 

def create_external_table(event,
                          gcp_project_id, 
                          bigquery_dataset, 
                          external_table_name, 
                          gcp_gcs_bucket, 
                          events_path):
    """
    Create an external table using the BigQueryCreateExternalTableOperator
    Parameters :
        gcp_project_id : str
        bigquery_dataset : str
        external_table_name : str
        gcp_gcs_bucket : str
        events_path : str
    
    Returns :
        task
    """
    task = BigQueryCreateExternalTableOperator(
        task_id = f'{event}_create_external_table',
        table_resource = {
            'tableReference': {
            'projectId': gcp_project_id,
            'datasetId': bigquery_dataset,
            'tableId': f'{external_table_name}',
            },
            'externalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{gcp_gcs_bucket}/{events_path}/*'],
            },
        }
    )

    return task