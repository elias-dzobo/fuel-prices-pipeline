"""
Transform Data from API

- average diesel price 
- average petrol price
- save to postgress 
"""
import os 
import pandas as pd 
from database import create_connection

path = '/Users/eliasdzobo/Documents/data-eng/fuel-pipeline/Data'

x = os.listdir(path)

df = pd.read_csv(os.path.join(path, x[0]))

avg_petrol = round(df['petrol'].mean(), 2)
avg_diesel = round(df['diesel'].mean(), 2)

print(avg_diesel)
print(avg_petrol)