import json
from kafka import KafkaProducer
import time
from extract_api import ExtractApi
import logging_config

# logging
logger = logging_config.logger

class SendKafka(ExtractApi) : 

    """ this class send extracted data to kafka producer"""
    
    def __init__(self, topic = 'finance') : 

        super().__init__()
        self.message_count = 0
        self.topic = topic

    def send_events(self, producer, topic = 'finance') :  


        """ this method send data to kafka """
        topic = self.topic 
        # appel  extract symbol héritage 

        logger.debug("send_events : extract symbols")
        print('extract symbols -------------------------------------------')
        self.extract_symbols() 
        
        # appel extract data héritage 
        logger.debug("send_events : producer is initialized")

        while True : 

            logger.debug("send data to kafka boucle while")

            data = self.extract_data()

            for info in data : 
                print('dans la bouce for -------------------------------------------')
                
                time.sleep(2)
                producer.send(topic, info)
                self.message_count += 1 
                print(f"send data to kafka {self.message_count}")

    

if __name__ == "__main__" : 

    send_kafka = SendKafka('finance')

    producer = KafkaProducer(bootstrap_servers = "localhost:9092", value_serializer = lambda x : json.dumps(x).encode("utf-8"))  
    send_kafka.send_events(producer) 
    
