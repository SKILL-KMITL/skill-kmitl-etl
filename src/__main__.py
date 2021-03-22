import os
import pandas as pd
from connection import init_connection
from etl.main import etl_file
import logging
from logging.handlers import RotatingFileHandler
from logging import handlers
import sys


# init logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log", mode='w'),
        logging.StreamHandler()
    ]
)

# init path
try:
  root_path = os.getcwd()
  source_path = os.path.join(root_path, "data/source")
  source_files = os.listdir(source_path)
  source_files = [file for file in source_files if file != '.gitkeep']

  if not len(source_files):
    logging.warning('Source directory is empty')
    exit(0)
    
except(Exception) as error:
  logging.error('Failed to load source file: %s' % error)
  raise FileNotFoundError('Failed to load source file', error)
  exit(1)

# startup app
try:
  def startup(cursor, connection):
    for file in source_files:
      hdf_path = os.path.join(source_path, file)
      df = pd.read_hdf(hdf_path)
      etl_file(cursor, connection, file, df)

except(Exception) as error:
  raise Exception("Startup failed", error)
  exit(1)
  
if __name__ == '__main__':
  # init connection
  cursor, connection = init_connection()

  # startup
  startup(cursor, connection)