# Setup Kafka VM

Kafka is set up in a Docker Compose configuration, which runs inside a virtual machine on Google Cloud Platform 

# Setup 

- SSH connection to VM 

- Environnement variables : External_VM_IP_Adress:9092

```ini
export KAFKA_ADDRESS=IP.ADD.RE.SS
```

- clone

```ini 
git clone https://github.com/ghiles10/GCP_Streaming-Crypto-ETL.git && \
cd kafka
``` 

- start container 

```ini 
docker-compose build && docker-compose up 
``` 

This will start the Kafka broker

