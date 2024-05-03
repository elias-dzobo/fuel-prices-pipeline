import requests as re
import json 
import pandas as pd 
import os
from dotenv import dotenv_values
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from helpers import * 
from datetime import datetime, timedelta

env = dotenv_values('.env')




#login details
EMAIL = env.get('EMAIL')
PASSWORD = env.get('PASSWORD')

#data save path [ chnage to s3 bucket]
path = '/Users/eliasdzobo/Documents/data-eng/fuel-pipeline/Data'

"""
This script logs into cedirates.com and scarpes data regarding fuel prices from several fuel stations in Ghana 
"""
@task()
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



@task()
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

@task()
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

@task()
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

#task()
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


with DAG('fuel-pipeline', schedule_interval=timedelta(days=1)) as dag:
    
    with TaskGroup('fetch_data', tooltip='Extract data from API') as fetch_data:
        url, scrape_url = return_urls()
        data = get_data(url, scrape_url)

    with TaskGroup('create_datafile', tooltip='Create CSV files for data') as createDatafile:
        filename = create_datafile(data) 

    with TaskGroup('transform_data', tooltip='Create data aggregates') as transformData:
        values = transform_data(filename)

    with TaskGroup('save_to_postgres', tooltip='Save aggregated data to psotgres databse') as save2postgres:
        save_to_postgres(filename)

    fetch_data >> createDatafile >> transformData >> save2postgres





""" if __name__ == '__main__':
    url, scrape_url = return_urls()

    print(url)
    print(scrape_url)
    data_list = get_data(url, scrape_url)

    file = create_datafile(data_list) 


    path = os.path.join(path, file)
    save_to_postgres(path)

    #filename = create_datafile(data_list) """









   