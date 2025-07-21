# Maharashtra-Real-Time-Air-Quality-ETL-Project
A serverless data pipeline that extracts real-time air quality data, transforms it using Python, and loads it into AWS Glue for querying via Amazon Athena.

## ✨ Features
- Serverless architecture using AWS Lambda
- Real-time air quality data from Data.gov.in
- Daily automated ETL with AWS EventBridge
- Data transformation and cleaning using Python
- Queryable structured output using Athena


## 🎯 Objective
To collect, transform, and analyze real-time air quality data of Indian state Maharashtra or it can be done for across Indian cities also using a scalable, fully managed AWS solution.


## 🏗️ Architecture

![ETL Architecture](./aqi_project.png)

> The pipeline consists of two Lambda functions for extraction and transformation, with data moving through S3 and AWS Glue to Athena.

## 🛠️ Tech Stack
- **Language**: Python
- **Cloud**: AWS
- **Data Source**:  https://www.data.gov.in/resource/real-time-air-quality-index-various-locations
- **Services**: Lambda, S3, Glue, Athena, EventBridge, S3 trigger(Load Lambda function)

## 🔁 ETL Pipeline

### 📥 Extract
- AWS Lambda triggered by EventBridge on hourly basis(maharastra_air_quality_extract function)
- Fetches data from Data.gov.in API
- Saves raw JSON to Amazon S3

### 🔧 Transform
- S3 event triggers another Lambda(maharastra_air_quality_load function)
- Cleans and enriches data like:
-  1.adding new columns like range, aqi_value, air_quality, data and time column.
-  2.pivot_time_city: To analyze how AQI values vary hourly across different cities.
-  3.pivot_date_city: To analyze day-by-day AQI variations per city.
-  4.pivot_city_pollutant: To analyze which pollutants dominate in which cities based on AQI contribution.
- Outputs structured CSV/Parquet to transformed S3 location

### 📤 Load
- AWS Glue Crawler infers schema from transformed data
- Data is cataloged in Glue
- Amazon Athena is used for SQL queries

## Modules used

| Module        | Description                                                                |
| ------------- | -------------------------------------------------------------------------- |
| `boto3`       | AWS SDK for Python — used to upload processed and pivoted data files to S3 |
| `json`        | Built-in module to handle and parse JSON responses from APIs               |
| `requests`    | Used to fetch air quality data from external APIs                          |
| `datetime`    | Built-in module to handle timestamps, extract dates and hours              |
| `pandas`      | Core library for data analysis, transformation, and pivot table generation |
| `io.StringIO` | Built-in module to handle in-memory CSV string conversion before uploading |

## IAM Roles required for lambda functions:
### 1. maharastra_air_quality_extract function
####       1.AmazonS3FullAccess
####       2.AWSLambdaRole

### 2. maharastra_air_quality_load_function
####       1.AmazonS3FullAccess
####       2.AWSLambdaRole


# project screenshots

<img width="1906" height="1020" alt="Screenshot 2025-07-20 175553" src="https://github.com/user-attachments/assets/3b1947b5-77d6-4f37-aca0-8f469b3d889b" />


<img width="1919" height="1071" alt="Screenshot 2025-07-20 193520" src="https://github.com/user-attachments/assets/8d107d57-1e47-4d9b-8bfa-34b6d2b293df" />



<img width="1919" height="894" alt="Screenshot 2025-07-20 193553" src="https://github.com/user-attachments/assets/6a23b91c-7e2c-427f-a44e-54374728ed7b" />



<img width="1919" height="1061" alt="Screenshot 2025-07-20 193621" src="https://github.com/user-attachments/assets/cf7f2e7b-9706-46ad-a373-f87c473f69b3" />



<img width="1919" height="1000" alt="Screenshot 2025-07-20 193643" src="https://github.com/user-attachments/assets/9e5c9370-7957-4cf9-8a44-44196f6c679b" />



<img width="1919" height="959" alt="Screenshot 2025-07-20 193705" src="https://github.com/user-attachments/assets/a317cd93-d2ce-489c-9142-e7c02b7e7b9f" />



<img width="1919" height="965" alt="Screenshot 2025-07-20 193721" src="https://github.com/user-attachments/assets/71af46d2-1f78-4be5-a4bc-8cac402ed936" />


<img width="1919" height="931" alt="Screenshot 2025-07-20 193738" src="https://github.com/user-attachments/assets/434f7de3-0547-4c89-9425-b1df4f8ef38e" />


<img width="1919" height="1023" alt="Screenshot 2025-07-20 193912" src="https://github.com/user-attachments/assets/9aac9f2d-34b5-478f-8827-242ee9459b72" />



<img width="1885" height="999" alt="Screenshot 2025-07-20 193927" src="https://github.com/user-attachments/assets/d6a8ab22-c6c7-4230-b6ce-2132eb625e3c" />



<img width="1919" height="1059" alt="Screenshot 2025-07-20 193949" src="https://github.com/user-attachments/assets/a5225ea7-ab1e-4831-9cb4-921ee668fd8f" />


<img width="1919" height="1073" alt="Screenshot 2025-07-20 194052" src="https://github.com/user-attachments/assets/fb44b860-d497-44d0-88be-8bb753089824" />


