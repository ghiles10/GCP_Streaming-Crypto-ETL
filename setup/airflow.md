# Setup Airflow VM

Airflow has been set up on Docker, and it is running on a dedicated compute instance.

Individual Airflow DAGs for each stage of the ETL pipeline. 

## Flow 

- a temporary table, a fact table and two dimension tables are created only once 
- the data of the temporary table are inserted in the final tables by respecting a specific schema
- the temporary table is deleted until airflow is triggered again

## Setup 

- clone repo 

``` ini 
git clone https://github.com/ghiles10/GCP_Streaming-Crypto-ETL.git
```

- Move the service account json file from local to the VM machine in ~/.google/credentials/ directory. 
- start airflow  :
 
``` ini 
airflow-up
```

- Airflow should be available on port 8080

