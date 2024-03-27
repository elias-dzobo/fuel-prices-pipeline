import requests 
import pandas as pd 
import os 
from dotenv import load_dotenv

load_dotenv()

#key for authenticating twitter bot 
API_KEY = os.getenv('API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY') 


data = {
    'company': ['A', 'B', 'C', 'D'],
    'price': [1,2,3,4],
    'rev': [100, 200, 300, 400]
}

df = pd.DataFrame(data)


tweet_template = """
Company  Price  Rev \n 
"""

for i in range(len(df)):
    a = list(df.loc[i])
    a = [str(i) for i in a]
    x = "     ".join(a)
    x += '\n'
    tweet_template += x 


print(tweet_template)