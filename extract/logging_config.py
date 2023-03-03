import logging
import os
import sys

if not os.path.exists("extract/log/"):
    os.mkdir("extract/log/") 

file_name = os.path.basename(sys.argv[0]).split(".")[0] 

logger = logging.getLogger(file_name)
handler = logging.FileHandler(f"extract/log/{file_name}.log", mode = 'w')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(module)s  - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)