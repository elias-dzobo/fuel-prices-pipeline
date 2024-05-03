"""
Transform Data from API

- average diesel price 
- average petrol price
- save to postgress 
"""
import os 
import pandas as pd 

path = '/Users/eliasdzobo/Documents/data-eng/fuel-pipeline/Data'

x = os.listdir(path)

df = pd.read_csv(os.path.join(path, x[0]))

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


print('avg diesel ', avg_diesel)
print('avg petrol ',avg_petrol)

print('min petrol ',min_petrol)
print('min diesel ',min_diesel)

print('max petrol ',max_petrol)
print('max diesel ',max_diesel)

print('min petrol station ',min_petrol_station)
print('min diesel station ',min_diesel_station)

print('max petrol station ',max_petrol_station)
print('max diesel station ',max_diesel_station)

