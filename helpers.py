from datetime import datetime 
import psycopg2
from dotenv import dotenv_values

env = dotenv_values('.env')

def format_date():

    date = datetime.now().strftime("%d-%m-%Y")
    date = datetime.now().strftime("%d-%m-%Y")

    date = date.split('-')
    
    if date[0][0] == '0':
        date[0] = date[0][1]

    if date[1][0] == '0':
        date[1] = date[1][1]
    

    date = '-'.join(date)

    return date 

def db_connection():
    conn = psycopg2.connect(
    dbname=env.get("DB_NAME"),
    user=env.get("DB_USER"),
    password=env.get("DB_PASSWORD"),
    host=env.get("DB_HOST"),
    port=env.get("DB_PORT")
    )

    return conn 



