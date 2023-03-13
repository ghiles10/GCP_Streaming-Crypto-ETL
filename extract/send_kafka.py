import json
from kafka import KafkaProducer
import time
from extract_api import ExtractApi
import logging_config

# Initialisation de la configuration de logging
logger = logging_config.logger

class SendKafka(ExtractApi):
    
    """ Class pour envoyer les données extraites à un producer Kafka"""

    def __init__(self, topic='finance'):

        # Appel au constructeur de la classe parente ExtractApi
        super().__init__()

        # Initialisation du compteur de messages envoyés et du nom du topic Kafka
        self.message_count = 0
        self.topic = topic

    def send_events(self, producer):

        """Méthode pour envoyer les données extraites à un producteur Kafka"""

        # Appel à la méthode extract_symbols de la classe parente
        logger.debug("send_events : extract symbols")
        self.extract_symbols() 

        # Initialisation du producteur Kafka
        logger.debug("send_events : producer is initialized")
        while True:

            # Extraction des données pour les symboles extraits
            data = self.extract_data()
            for info in data:
                time.sleep(3)
                # Envoi des données au producteur Kafka
                producer.send(self.topic, info)
                self.message_count += 1
                logger.debug(f"sent message {self.message_count}")
    
# Bloc principal
if __name__ == "__main__":
    
    # Création de l'objet SendKafka
    send_kafka = SendKafka('finance')
    # Initialisation du producteur Kafka
    producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda x: json.dumps(x).encode("utf-8"))  
    # Envoi des données extraites au producteur Kafka
    send_kafka.send_events(producer)
