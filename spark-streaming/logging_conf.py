import logging
import os
import sys

if not os.path.exists("spark-streaming/log/"):
    os.mkdir("spark-streaming/log/")

file_name = os.path.basename(sys.argv[0]).split(".")[0]

logger = logging.getLogger(file_name)
handler = logging.FileHandler(f"extract/log/{file_name}.log", mode="w")
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s  - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
