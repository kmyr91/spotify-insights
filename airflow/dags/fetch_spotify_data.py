from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import base64 
from dotenv import load_dotenv
import os

load_dotenv()

def fetch_spotify_data():
    logging.info("Starting to fetch Spotify data")
    
    # Setting up client credentials
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    
    # Encoding client credentials
    encoded_credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    # Get the token
    headers = {
        "Authorization": f"Basic {encoded_credentials}"
    }
    payload = {
        "grant_type": "client_credentials"
    }
    response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=payload)
    response_data = response.json()
    access_token = response_data['access_token']
    
    # Fetching new releases
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    new_releases = requests.get("https://api.spotify.com/v1/browse/new-releases", headers=headers).json()
    
    logging.info(f"Fetched new releases: {new_releases}")
    for album in new_releases['albums']['items']:
        logging.info(f"{album['name']} - {album['artists'][0]['name']}")
    return new_releases

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'hello_spotify_dag',
    default_args=default_args,
    description='A simple DAG to test Spotify API',
    schedule=timedelta(days=1),
)

fetch_spotify_data_task = PythonOperator(
    task_id='fetch_spotify_data',
    python_callable=fetch_spotify_data,
    dag=dag,
)

fetch_spotify_data_task
