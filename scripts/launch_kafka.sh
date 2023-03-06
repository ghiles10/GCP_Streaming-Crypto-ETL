#!bin/bash 

echo " Launching Zookeeper "
bin/zookeeper-server-start.sh config/zookeeper.properties

echo " Launching Kafka " 
bin/kafka-server-start.sh config/server.properties