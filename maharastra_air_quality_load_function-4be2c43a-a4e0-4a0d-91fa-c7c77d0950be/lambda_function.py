import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def airquality(data):
    air_quality_list=[]
    for row in data:
        country = row['country']
        state = row['state']
        city = row['city']
        station= row['station']
        last_update = row['last_update']
        latitude = row['latitude']
        longitude = row['longitude']
        pollutant_id = row['pollutant_id']
        min_value = row['min_value']
        max_value = row['max_value']
        avg_value = row['avg_value']

        air_quality_data = {'country':country, 'state':state, 'city':city, 'station':station,
                            'last_update':last_update,'latitude':latitude, 'longitude':longitude,
                            'pollutant_id':pollutant_id,'min_value':min_value,'max_value':max_value,
                            'avg_value':avg_value}
        air_quality_list.append(air_quality_data)
    return air_quality_list


def lambda_handler(event, context):
    #first importing raw files 
    s3 = boto3.client('s3')
    Bucket = "maharashtra-aqi-etl-project-abhishekram"
    Key = "raw_air_quality_data/to_be_processed/"

    aqi_data = []
    aqi_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        #print(file['Key'])
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            aqi_data.append(jsonObject)
            aqi_keys.append(file_key)

    for data in aqi_data:
        air_quality_list = airquality(data)

        air_quality_df = pd.DataFrame.from_dict(air_quality_list)

        air_quality_df['last_update']= pd.to_datetime(air_quality_df['last_update'],format="%d-%m-%Y %H:%M:%S")

        cols = ['min_value','max_value','avg_value','longitude','latitude']
        for col in cols:
            air_quality_df[col] = pd.to_numeric(air_quality_df[col],errors='coerce')


        air_quality_df.dropna(inplace=True)

        air_quality_df['range'] = air_quality_df['max_value'] - air_quality_df['min_value']

        aqi_breakpoints = {
            'PM2.5': [
                (0, 30, 0, 50),
                (31, 60, 51, 100),
                (61, 90, 101, 200),
                (91, 120, 201, 300),
                (121, 250, 301, 400),
                (251, 500, 401, 500)
            ],
            'PM10': [
                (0, 50, 0, 50),
                (51, 100, 51, 100),
                (101, 250, 101, 200),
                (251, 350, 201, 300),
                (351, 430, 301, 400),
                (431, 500, 401, 500)
            ],
            'NO2': [
                (0, 40, 0, 50),
                (41, 80, 51, 100),
                (81, 180, 101, 200),
                (181, 280, 201, 300),
                (281, 400, 301, 400),
                (401, 1000, 401, 500)
            ],
            'SO2': [
                (0, 40, 0, 50),
                (41, 80, 51, 100),
                (81, 380, 101, 200),
                (381, 800, 201, 300),
                (801, 1600, 301, 400),
                (1601, 2000, 401, 500)
            ],
            'CO': [
                (0, 1.0, 0, 50),
                (1.1, 2.0, 51, 100),
                (2.1, 10, 101, 200),
                (10.1, 17, 201, 300),
                (17.1, 34, 301, 400),
                (34.1, 50, 401, 500)
            ],
            'OZONE': [
                (0, 50, 0, 50),
                (51, 100, 51, 100),
                (101, 168, 101, 200),
                (169, 208, 201, 300),
                (209, 748, 301, 400),
                (749, 1000, 401, 500)
            ],
            'NH3': [
                (0, 200, 0, 50),
                (201, 400, 51, 100),
                (401, 800, 101, 200),
                (801, 1200, 201, 300),
                (1201, 1800, 301, 400),
                (1801, 3000, 401, 500)
            ]
        }

        def calculate_aqi(pollutant, avg_value):
            for bp in aqi_breakpoints[pollutant]:
                B_low, B_high, I_low, I_high = bp
                if B_low <= avg_value <= B_high:
                    aqi = I_low + ((I_high - I_low) / (B_high - B_low)) * (avg_value - B_low)
                    return round(aqi)
            return None # if avg_value is outside all ranges

        def aqi_from_row(row):
            return calculate_aqi(row['pollutant_id'],row['avg_value'])

        air_quality_df['aqi_value'] = air_quality_df.apply(aqi_from_row, axis=1)

        def get_quality_label(aqi):
            if pd.isna(aqi):
                return "Unknown"
            elif aqi <= 50:
                return "Good"
            elif aqi <= 100:
                return "Satisfactory"
            elif aqi <= 200:
                return " Moderate"
            elif aqi <= 300:
                return "Poor"
            elif aqi <= 400:
                return "Very Poor"
            else:
                return "Severe"

        air_quality_df['air_quality'] = air_quality_df['aqi_value'].apply(get_quality_label)

        air_quality_df['date'] = air_quality_df['last_update'].dt.date
        air_quality_df['time'] = air_quality_df['last_update'].dt.time

        #Pivoting table 

        pivot_city_pollutant = air_quality_df.pivot_table(
            index = 'city',
            columns = 'pollutant_id',
            values = 'aqi_value',
            aggfunc = 'mean'
        )

        #pivot date wise aqi trends by city
        pivot_date_city = air_quality_df.pivot_table(
            index='city',
            columns='date',
            values='aqi_value',
            aggfunc='mean'
        )

        #pivot time wise aqi trends by city
        pivot_time_city = air_quality_df.pivot_table(
            index='city',
            columns='time',
            values='aqi_value',
            aggfunc='mean'
        )


        aqi_transformed_key = "transformed_air_quality_data/transformed_dataset_new/maha_aqi_transformed_"+ str(datetime.now()) + ".csv"
        aqi_buffer = StringIO()
        air_quality_df.to_csv(aqi_buffer, index=False)
        air_quality_content = aqi_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=aqi_transformed_key, Body=air_quality_content)


        pivot_city_key = "transformed_air_quality_data/pivot_city_pollutant/pivot_city_pollutant_"+ str(datetime.now()) + ".csv"
        pivot_city_buffer = StringIO()
        pivot_city_pollutant.to_csv(pivot_city_buffer, index=False)
        pivot_city_content = pivot_city_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=pivot_city_key, Body=pivot_city_content)

        pivot_date_key = "transformed_air_quality_data/pivot_date_city/pivot_date_city_"+ str(datetime.now()) + ".csv"
        pivot_date_buffer = StringIO()
        pivot_date_city.to_csv(pivot_date_buffer, index=False)
        pivot_date_content = pivot_date_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=pivot_date_key, Body=pivot_date_content)

        pivot_time_key ="transformed_air_quality_data/pivot_time_city/pivot_time_city_" + str(datetime.now()) + ".csv"
        pivot_time_buffer = StringIO()
        pivot_time_city.to_csv(pivot_time_buffer, index=False)
        pivot_time_content = pivot_time_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=pivot_time_key, Body=pivot_time_content)

       
     #first copying the to_be_processed files to processed files and deleting the to_be_processed files
     # aqi_keys = [] will be used for this case 
    s3_resource = boto3.resource('s3')
    for key in aqi_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_air_quality_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket,key).delete()

        

