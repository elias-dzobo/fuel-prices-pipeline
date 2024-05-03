import boto3 
import pandas as pd 
import time 
import redshift_connector 
from datetime import datetime
import os 
from dotenv import load_dotenv

load_dotenv()

"""
This script gets the persisted data from the S3 bucket and creates the schema in Redshift 
"""

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_TRANSFORMED_BUCKET = os.getenv('S3_TRANSFORMED_BUCKET')
HOST = os.getenv('HOST')
DATABASE = os.getenv('DATABASE')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')

date = datetime.now().strftime("%d-%m-%Y")
date = date.split('-')
date[1] = date[1][1]

date = '-'.join(date)

s3 = boto3.client('s3',
                  aws_access_key_id = AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

filename = 'fuelprices_{date}.csv'

s3.download_file("elias-fuel-bucket", 'rawdata/{}'.format(filename), './{}'.format(filename))

prices = pd.read_csv(filename)

pricesSchema = pd.io.sql.get_schema(prices, 'prices')

conn = redshift_connector.connect(
    host=HOST,
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    port=5439
)

conn.autocommit = True 

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute(pricesSchema)

conn.close() 