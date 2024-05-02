import requests as re
import json 
from datetime import datetime 
import pandas as pd 
import os
from dotenv import dotenv_values
import psycopg2


env = dotenv_values('.env')




#login details
EMAIL = env.get('EMAIL')
PASSWORD = env.get('PASSWORD')

path = '/Users/eliasdzobo/Documents/data-eng/fuel-pipeline/Data'

"""
This script logs into cedirates.com and scarpes data regarding fuel prices from several fuel stations in Ghana 
"""

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



def return_urls():
    """
    A function to standardise and return a the login url and scrape url for each days prices 

    Input: None 

    Output: 
        String 
    """
    

    date = format_date()

    url = "https://cedirates.com/api/v1/auth/login"
    scrape_url = "https://cedirates.com/api/v1/fuelPrices/{}".format(date) 


    return url, scrape_url


def get_data(url, scrape_url):
    """
    A function that makes a request to login and scrape from data source

    input:
        url: String 
        scrape_url: String

    Returns:
        List
    """

    data = {"email":EMAIL,"password":PASSWORD}

    data = json.dumps(data)

    headers = {"Content-type": 'application/json'}

    with re.Session() as session:

        response = session.post(url, headers=headers, data=data)

        if response.ok:
            print('Login Successful')
        else:
            print('Login Failed')

        response = session.get(scrape_url)

        data = response.json()

        data_list = data['fuelPrices']

        return data_list

def create_datafile(data_list):
    """
    A function that converts the scrape output into a csv file 

    Input:
        data_list: List 

    Returns:
        filename: String
    """
    fuel_data = {
        "company": [],
        "petrol": [],
        "diesel": []
    }

    for entry in data_list:
        fuel_data["company"].append(entry['company']['companyName'])
        fuel_data["diesel"].append(entry['diesel'])
        fuel_data['petrol'].append(entry['petrol'])

    date = format_date()

    filename = f'fuelprices_{date}.csv'

    pd.DataFrame(fuel_data).to_csv(os.path.join(path, filename))

    return filename

def transform_data(filename):
    df = pd.read_csv(filename)

    date = format_date()

    avg_petrol = round(df['petrol'].mean(), 2)
    avg_diesel = round(df['diesel'].mean(), 2)

    min_petrol = min(df['petrol']) 
    min_diesel = min(df['diesel'])

    max_petrol = max(df['petrol'])
    max_diesel = max(df['diesel'])

    min_petrol_station = df[df['petrol'] == min_petrol].values[0][1]
    min_diesel_station = df[df['diesel'] == min_diesel].values[0][1]

    max_petrol_station = df[df['petrol'] == max_petrol].values[0][1]
    max_diesel_station = df[df['diesel'] == max_diesel].values[0][1]

    ## save transformed metrics

    return (date, avg_petrol, avg_diesel, min_petrol, min_diesel, max_petrol, max_diesel, min_petrol_station, min_diesel_station, max_petrol_station, max_diesel_station)

def save_to_postgres(filename):
    conn = db_connection()
    curr = conn.cursor()

    insert_query = """
    INSERT INTO fuel_data 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values_to_insert = transform_data(filename) 

    try:

        curr.execute(insert_query, values_to_insert)

        conn.commit()

        curr.close()
        conn.close()

        return True 
    except Exception as e:
        print('Exception: ', e)
        return False 





if __name__ == '__main__':
    url, scrape_url = return_urls()

    print(url)
    print(scrape_url)
    data_list = get_data(url, scrape_url)

    file = create_datafile(data_list) 


    path = os.path.join(path, file)
    save_to_postgres(path)

    #filename = create_datafile(data_list)









   