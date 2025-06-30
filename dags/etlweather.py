# from airflow import DAG  
# #read from api we use hook 
# from airflow.providers.http.hooks.http import HttpHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.decorators import task 
# # from airflow.utils.dates import days_ago removed in new versions
# import requests
# import json
# #kind of task we are doing.
# from datetime import datetime, timedelta

# # default_args = {
# #     'start_date': datetime.now() - timedelta(days=1)
# # }
# # With help of latitude and longitude we will get the weather...

# LATITUDE = '51.5074'
# LONGITUDE = '-0.1278'

# POSTGRES_CONN_ID ='postgres_default'
# API_CON_ID='open_meteo_api'


# default_args={
#     'owner':'airflow',
#     'start_date':datetime.now() - timedelta(days=1),
# }

# #Create DAG

# with DAG(dag_id='weather_etl_pipeline',
#          default_args=default_args,
#         #  schedule_interval='@daily', removed in newer versions
#          schedule='@daily',
#          catchup=False
#          )as dag:
#     @task()
#     def extract_weather_data():
#         """"Extract weather data fro, Open-Meteo API using Airflow COnnection."""
        
#         #use HTTP Hook to get the connection details from Airflow connection
        
#         http_hook = HttpHook(http_conn_id = API_CON_ID,method='GET')
        
#         #Build the API endpoint
#         ##https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
#         endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
#         # Make the request via HHHP hook
#         response=http_hook.run(endpoint)
        
#         if response.status_code == 200:
#             return response.json()
#         else:
#             raise Exception(f"Failed to fetch weather data: {response.status_code}")
    
#     @task()
#     def transform_weather_data(weather_data):
#         """Transform the extracted weather data."""
        
#         current_weather = weather_data['current_weather']
#         transformed_data = {
#             'latitude': LATITUDE,
#             'longitude': LONGITUDE,
#             'temperature': current_weather['temperature'],
#             'windspeed': current_weather['windspeed'],
#             'winddirection': current_weather['winddirection'],
#             'weathercode': current_weather['weathercode']
#         }
#         return transformed_data
    
#     @task()
#     def load_weather_data(transformed_data):
#         """Load transformed data into postgreSQL."""
        
#         pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         conn = pg_hook.get_conn()
#         cursor = conn.cursor()
        
#         #Create table if it doesn't exist
#         cursor.execute("""
#                        CREATE TABLE IF NOT EXISTS weather_data (
#                            latitude FLOAT,
#                            longitude FLOAT,
#                            temperature FLOAT,
#                            windspeed FLOAT,
#                            winddirection FLOAT,
#                            waethercode INT,
#                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#                        );
#                        """)
        
#         # Insert transformed data into the table
#         cursor.execute("""
#                        INSERT INTO weather_data(latitude, longitude,temperature, windspeed, winddirection,weathercode)
#                        VALUES (%s,%s,%s,%s,%s,%s)              
#                        """, (
#                            transformed_data['latitude'],
#                            transformed_data['longitude'],
#                            transformed_data['temperature'],
#                            transformed_data['windspeed'],
#                            transformed_data['winddirection'],
#                            transformed_data['weathercode']
                           
#                        ))
#         conn.commit()
#         cursor.close()
        
#         #Data workflow - ETL pipeline
# weather_data = extract_weather_data()
# transformed_data=transform_weather_data(weather_data)
# load_weather_data(transformed_data)
from airflow import DAG  
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task 
from datetime import datetime, timedelta

# Constants
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID ='postgres_default'
API_CON_ID='open_meteo_api'

# Default args
default_args={
    'owner':'airflow',
    'start_date':datetime.now() - timedelta(days=1),
}

# DAG definition
with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using HttpHook."""
        http_hook = HttpHook(http_conn_id=API_CON_ID, method='GET')
        endpoint = f'v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        return {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }

    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert data
        cursor.execute("""
            INSERT INTO weather_data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    # Task execution sequence
    raw_data = extract_weather_data()
    processed_data = transform_weather_data(raw_data)
    load_weather_data(processed_data)
