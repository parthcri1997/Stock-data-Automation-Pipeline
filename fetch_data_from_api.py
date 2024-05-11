import json
import time

import requests
import boto3
import datetime

# Write json to s3 taken from here : https://stackoverflow.com/questions/46844263/writing-json-to-file-in-s3-bucket
symbols = ['IBM','ACN','DELL','AAPL','TSLA']
session = boto3.session.Session(profile_name='default')
client = boto3.client('secretsmanager',region_name = 'us-east-1')
s3_client = boto3.client('s3',region_name = 'us-east-1')
response = client.get_secret_value(SecretId='Stock_data_API_Key_Using_CF')
api_key = response['SecretString']
print("Fetching data from API")
for symbol in symbols:
    url = f'https://api.stockdata.org/v1/data/eod?symbols={symbol}&api_token={api_key}'
    req = requests.get(url=url)
    data = req.json()
    print("Fetching stock data for stock ",symbol," for date:",data["data"][0]["date"])
    latest_date = datetime.datetime.strptime(data["data"][0]["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
    # s3_object = s3_client.Object(bucket_name = 'stockdatatermassignment5409', key= 'json/'+f'{symbol}'+'_' + datetime.datetime.strftime(latest_date,'%Y-%m-%d') + '.json')
    s3_client.put_object(Body=json.dumps(data).encode('UTF-8'),Bucket = 'stockdatatermassignment5409',Key= 'json/'+f'{symbol}'+'_' + datetime.datetime.strftime(latest_date,'%Y-%m-%d') + '.json')
    time.sleep(60)
    print("Fetched stock data for stock ", symbol, " for date:", data["data"][0]["date"])
print("Data fetched successfully and stored on s3.")
