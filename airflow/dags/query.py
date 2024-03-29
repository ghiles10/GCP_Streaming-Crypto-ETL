import os

QUERY_FACT = f"""
SELECT time, symbol, volvalue, high, low
FROM {os.environ.get("PROJECT_ID")}.{os.environ.get("DATASET_ID")}.temp_table ; 
"""

QUERY_DIM_TIME = f"""
SELECT time, year, month, day, hour
FROM {os.environ.get("PROJECT_ID")}.{os.environ.get("DATASET_ID")}.temp_table ; 
"""

QUERY_DIM_STOCK = f"""
SELECT symbol, buy, sell, changerate, changeprice, vol
FROM {os.environ.get("PROJECT_ID")}.{os.environ.get("DATASET_ID")}.temp_table ; 
"""
