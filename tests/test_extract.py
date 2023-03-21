import sys 
sys.path.append(r"/workspaces/GCP_Streaming-Crypto-ETL") 

import responses 
from extract.extract_api import ExtractApi 

@responses.activate
def test_coherance_extract_symbols(): 

    """ Test de la cohérence de la class ExtractApi"""

    # Initialisation de la réponse
    responses.add(responses.GET, "https://api.binance.com/api/v3/ticker/price", 
    json=[ {"symbol": "BTCUSDT", "price" : "10000"}, {"symbol": "ETHEUR" , "price" : "1000"}], 
    status=200) 

    # Création de l'objet ExtractApi
    extract_api = ExtractApi() 
    # Extraction des symboles de Binance
    extract_api.extract_symbols() 
    # Vérification de la cohérence des symboles
    assert extract_api.symbols == ["BTCUSDT"] 

    # Initialisation de la réponse 
    responses.add(responses.GET, "https://api.kucoin.com/api/v1/market/stats?symbol=BTC-USDT",
                                json={ 'data' : {"symbol": "BTCUSDT", "price" : "10000"}  },
                                status=200) 

    # test extraction des données de l'API KuCoin
    data = extract_api.extract_data() 
    assert data[0] == {"symbol": "BTCUSDT", "price" : "10000"} 
