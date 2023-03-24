# Setup Airflow VM

Airflow has been set up on Docker, and it is running on a dedicated compute instance.

Individual Airflow DAGs for each stage of the ETL pipeline, including data collection, data processing, and data warehousing.

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

