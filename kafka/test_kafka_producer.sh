#!/bin/bash

# vérification création topic
docker exec -it kafka-tools kafka-topics --create --topic teste1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# vérufucation création procuteur 
docker exec kafka-tools bash -c "seq 100 | kafka-console-producer --request-required-acks 1 --broker-list localhost:9092 --topic teste1 && echo 'Produced 100 messages.'"


