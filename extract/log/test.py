from kafka import KafkaConsumer
import json


cons = KafkaConsumer('finance', bootstrap_servers = "localhost:9092", value_deserializer = lambda x : json.loads(x.decode("utf-8")))

for m in cons:
    # any custom logic you need
    print(m.value) 

    