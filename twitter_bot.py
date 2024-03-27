
import pandas as pd 
import os 
from dotenv import load_dotenv
import tweepy
from datetime import datetime
import boto3

load_dotenv()

#key for authenticating twitter bot 
API_KEY = os.getenv('API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY') 
# Twitter API credentials
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')
#s3 credentials 
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


date = datetime.now().strftime("%d-%m-%Y")
date = date.split('-')
date[1] = date[1][1]

date = '-'.join(date)

# get s3 client
s3 = boto3.client('s3',
                  aws_access_key_id = AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


filename = 'fuelprices_{}'.format(date)

s3.download_file("elias-fuel-bucket", 'rawdata/{}'.format(filename), './{}'.format(filename))

df = pd.read_csv(filename)

#convert csv file in string template 
tweet_message = ""





# Authenticate to Twitter
auth = tweepy.OAuth1UserHandler(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

def tweet(message):
    try:
        api.update_status(message)
        print("Tweeted:", message)
    except tweepy.TweepError as e:
        print("Error:", e)



if __name__ == "__main__":
    tweet(tweet_message)
