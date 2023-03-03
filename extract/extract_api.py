import requests
import json


class ExtractApi : 

    def __init__(self) :

        self.symbols = [] 


    def extract_symbols(self, path = "https://api.binance.com/api/v3/ticker/price") : 

        """ this method extract all symbols from binance api that ends with USDT"""

        response = requests.get(path, timeout = 5) 
        
        if response.status_code == 200 :
            response = json.loads(response.text) 
            
            for symbol in response :
                for v in symbol.values() : 
                    if v.endswith("USDT") : 
                        self.symbols.append(v) 
    
    def extract_data(self, path = "https://api.kucoin.com/api/v1/market/stats?symbol=" ) :  
            
            """ this method extract data from kucoin api """

            for symbol in self.symbols :

                response = requests.get(path + f"{symbol[:-4]}-USDT", timeout = 5) 
        
                if response.status_code == 200 : 
                    response = json.loads(response.text) 
                    print(response)




if __name__ == "__main__" : 

    extract_api = ExtractApi()
    extract_api.extract_symbols()
    extract_api.extract_data() 