import requests
import json
import logging_config

# Initialisation de la configuration de logging
logger = logging_config.logger

class ExtractApi:

    """ Class pour extraire les données de l'API Binance et de l'API KuCoin"""
    
    def __init__(self):
        # Initialisation de la liste de symboles
        self.symbols = []

    def extract_symbols(self, path="https://api.binance.com/api/v3/ticker/price"):

        """Méthode pour extraire tous les symboles de l'API Binance qui se terminent par USDT"""

        response = requests.get(path, timeout=5) 
        # Vérification du code de statut de la réponse
        if response.status_code == 200:
            logger.debug("extract_symbols : response status code is 200")

            # Conversion de la réponse en JSON
            response = json.loads(response.text) 

            # Parcours des symboles de la réponse
            for symbol in response:
                for value in symbol.values(): 
                    # Ajout des symboles se terminant par USDT
                    if value.endswith("USDT"): 
                        self.symbols.append(value) 
    
    def extract_data(self, path="https://api.kucoin.com/api/v1/market/stats?symbol="):  

        """Méthode pour extraire les données de l'API KuCoin"""

        logger.debug("extracting finance data")
        # Limitation à 100 symboles maximum
        self.symbols = list(set(self.symbols))
        data = [] 

        # Parcours des symboles
        for symbol in self.symbols:
            response = requests.get(path + f"{symbol[:-4]}-USDT", timeout=5) 

            # Vérification du code de statut de la réponse
            if response.status_code == 200:
                # Ajout des données au format JSON
                data.append(json.loads(response.text)['data']) 

        return data  
            
# Bloc principal
if __name__ == "__main__":
    
    # Création de l'objet ExtractApi
    extract_api = ExtractApi()
    # Extraction des symboles de Binance
    extract_api.extract_symbols()
    # Extraction des données de KuCoin pour les symboles extraits
    data = extract_api.extract_data()
