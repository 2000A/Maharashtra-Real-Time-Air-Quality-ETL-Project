import json
import requests
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):

    api_keys = os.environ.get('api_key')
    api_key = api_keys
    resource_id = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
    url = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"

    params = {
        "api-key": api_key,
        "format": "json",
        "offset": 0,
        "limit": 1500,
        "filters[state]": "Maharashtra"
    }

    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data1 = response.json()
        data = data1["records"]
        
    #dumping entire json file onto s3 

    client = boto3.client('s3')

    filename = "mah_air_quality_" + str(datetime.now()) + ".json"

    client.put_object(
        Bucket="maharashtra-aqi-etl-project-abhishekram",
        Key="raw_air_quality_data/to_be_processed/" + filename,
        Body=json.dumps(data)
    )
