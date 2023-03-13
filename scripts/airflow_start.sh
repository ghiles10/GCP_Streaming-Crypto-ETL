#!/bin/bash

echo "build airflow docker images"
cd ~/airflow
docker-compose build

echo "run airflow-init"
docker-compose up airflow-init

echo "start airflow"
docker-compose up -d

echo "Airflow started successfully in detached mode. Port 8080 is exposed for the webserver"
echo "Airflow UI: http://localhost:8080/admin/" 
