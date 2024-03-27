import redshift_connector
import os 
from dotenv import load_dotenv

load_dotenv()

"""
This script copies the "transformed" data fro  s3 to Redshift
"""


HOST = os.getenv('HOST')
DATABASE = os.getenv('DATABASE')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')

conn = redshift_connector.connect(
    host=HOST,
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    port=5439
)

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""
copy prices from 's3://elias-fuel-bucket/rawdata/
credentials '<ROLE_ASSOCIATED_FOR_REDSHIFT>'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
               """)


conn.close()