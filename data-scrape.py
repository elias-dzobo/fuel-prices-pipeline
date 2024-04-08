import requests as re
from pprint import pprint
import json 
from bs4 import BeautifulSoup
from datetime import datetime 
import pandas as pd 
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
#login details
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

path = '/Users/eliasdzobo/Documents/data-eng/fuel-pipeline/Data'

"""
This script logs into cedirates.com and scarpes data regarding fuel prices from several fuel stations in Ghana 
"""

date = datetime.now().strftime("%d-%m-%Y")

s3 = boto3.client('s3')



def return_urls():
    """
    A function to standardise and return a the login url and scrape url for each days prices 

    Input: None 

    Output: 
        String 
    """
    date = datetime.now().strftime("%d-%m-%Y")

    date = date.split('-')
    date[1] = date[1][1]
    date[0] = date[0][1]

    date = '-'.join(date)

    url = "https://cedirates.com/api/v1/auth/login"
    #scrape_url = "https://cedirates.com/api/v1/fuelPrices/{}".format(date) 

    scrape_url = "https://cedirates.com/api/v1/fuelPrices/8-4-2024"

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

    filename = f'fuelprices_{date}.csv'

    pd.DataFrame(fuel_data).to_csv(os.path.join(path, filename))

    return filename


def save2S3bucket(filename):
    """
    A function that saves the data in csv format into an s3 bucket 

    Input:
        filename: String 

    Returns 
        Bool
    """

    try: 
        #do exception handling 
        s3.upload_file(filename, "elias-fuel-bucket", 'rawdata/{}'.format(filename))

        return True

    except Exception as e:
        return False


if __name__ == '__main__':
    url, scrape_url = return_urls()

    data_list = get_data(url, scrape_url)

    filename = create_datafile(data_list)

    #save2S3bucket(filename)






   