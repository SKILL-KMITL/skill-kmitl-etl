import sys
import psycopg2
import os
import logging
from environment import POSTGRES_HOST, POSTGRES_PASSWORD

def init_connection():
  try:
    connection = psycopg2.connect(host=POSTGRES_HOST, user="postgres", password=POSTGRES_PASSWORD, database="postgres")
    cursor = connection.cursor()
    logging.info('Connection database successfully')
    return cursor, connection
    
  except(Exception, psycopg2.Error) as error:
    logging.error('Failed to connect database: %s' % error)
    raise ConnectionError("Failed to connect database", error)
    exit(1)