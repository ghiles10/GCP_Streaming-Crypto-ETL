import json
from kafka import KafkaProducer
import time
from extract_api import ExtractApi
import logging_config

# logging
logger = logging_config.logger

class SendKafka(ExtractApi) : 

    def __init__(self, topic = 'finance') : 

        super().__init__()
        self.message_count = 0
        self.topic = topic

    def send_events(self, producer, topic = 'finance') :  


        """ this method send data to kafka """
        topic = self.topic 
        # appel  extract symbol héritage 

        logger.debug("send_events : extract symbols")
        self.extract_symbols() 
        
        # appel extract data héritage 
        # initialisation producer : KafkaProducer(bootstrap_servers = "localhost:9092", value_serializer = lambda x : json.dumps(x).encode("utf-8")) 
        logger.debug("send_events : producer is initialized")

        while True : 

            self.message_count += 1 

            logger.debug("send data to kafka boucle while")
            producer.send(topic, self.extract_data())
            time.sleep(3)

if __name__ == "__main__" : 

    send_kafka = SendKafka('finance')

    producer = KafkaProducer(bootstrap_servers = "localhost:9092", value_serializer = lambda x : json.dumps(x).encode("utf-8"))  
    send_kafka.send_events(producer) 
    
