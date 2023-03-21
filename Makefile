airflow_directory = ./airflow 
REQS=tests/requirements_tests.txt

###################### Infrastructure as Code setup    
tf-init:                             
		terraform -chdir=./terraform init
                                                                         
tf-apply:                            
		terraform -chdir=./terraform apply

tf_plan:
		terraform -chdir=./terraform plan

infra-down:
		terraform -chdir=./terraform destroy

infra-up: tf-init tf-apply 

######################## testing & linting setup

install: 
	pip install -r $(REQS)

format  : install
	pip install black
	black workspaces/GCP_Streaming-Crypto-ETL/	

test : install
	pytest -v tests/

lint: install
	pip install pylint
	-pylint --disable=R,C GCP_Streaming-Crypto-ETL/

ci: install format test lint 

###################### airflow docker setup 

docker-build:
		cd $(airflow_directory) && docker compose build

airflow-init:
		cd $(airflow_directory) && docker-compose up airflow-init

airflow-down:
		cd $(airflow_directory) && docker compose down

airflow-start:
		cd $(airflow_directory) && docker-compose up -d 

airflow-stop: 
		cd $(airflow_directory) && docker-compose stop 

airflow-up : docker-build airflow-init airflow-start 


######################Execution 
send-data:
		cd scripts && ./launch_kafka.sh  

preprocess-data:
		cd scripts && ./spark_launch.sh  
