from datetime import datetime
import json
from kafka import KafkaProducer
import time
from extract_api import ExtractApi


class SendKafka(ExtractApi) : 

    def __init__(self) : 

        super().__init__()
        # self.producer = KafkaProducer(bootstrap_servers = "localhost:9092", value_serializer = lambda x : json.dumps(x).encode("utf-8")) 

if __name__ == "__main__" : 

    send_kafka = SendKafka()
    send_kafka.extract_symbols()
    print(send_kafka.symbols) 