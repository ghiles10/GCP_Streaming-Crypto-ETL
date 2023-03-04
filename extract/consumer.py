from kafka import KafkaConsumer
import json

# Création d'un consommateur Kafka
consumer = KafkaConsumer('finance', bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Lecture des messages du topic "finance"
for message in consumer:
    print(message.value)
