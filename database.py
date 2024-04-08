import psycopg2
import os 
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def create_connection():
    conn = psycopg2.connect(database='postgres', user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    conn.autocommit = True 
    cursor = conn.cursor()
    return conn, cursor 

def create_database():
    conn, cursor = create_connection()
    cursor.execute(f'CREATE DATABASE {DB_NAME}')
    cursor.close()
    conn.close()

def create_table():
    conn, cursor = create_connection()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fueldata (
        date DATE PRIMARY KEY DEFAULT CURRENT_DATE,
        diesel FLOAT,
        petrol FLOAT
        )
        """
    )
    cursor.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    create_database()
    create_table()