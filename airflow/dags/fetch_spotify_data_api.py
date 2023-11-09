from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import logging
import base64
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from airflow.configuration import conf


AZURE_CONNECTION_STRING = conf.get('azure', 'blob_storage_conn_string')

def fetch_spotify_data_api(endpoint):
    logging.info(f"Starting to fetch Spotify data from {endpoint}")

    client_id = conf.get('spotify', 'client_id')
    client_secret = conf.get('spotify', 'client_secret')

    if not client_id or not client_secret:
        logging.error("Client ID or Client Secret is not set. Please check your Airflow Variables.")
        return

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

    if response.status_code != 200:
        logging.error(f"Failed to get access token from Spotify API. Status Code: {response.status_code}")
        logging.error(f"Response: {response.text}")
        return

    response_data = response.json()

    if 'access_token' not in response_data:
        logging.error("Access token not found in the response.")
        return

    access_token = response_data['access_token']

    results = []
    next_page_url = f"https://api.spotify.com/v1/{endpoint}"

    while next_page_url:
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        api_response = requests.get(next_page_url, headers=headers)

        if api_response.status_code != 200:
            logging.error(f"Failed to fetch data from endpoint {endpoint}. Status Code: {api_response.status_code}")
            return results

        data = api_response.json()
        albums = data.get('albums', {})
        results.extend(albums.get('items', []))

        # Check if there is a next page
        next_page_url = albums.get('next')

    logging.info(f"Completed fetching data from {endpoint}")
    return results



def check_and_create_container(connection_string, container_name):
    try:
        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)

        # Check if the container exists
        try:
            container_properties = container_client.get_container_properties()
            print(f"Container '{container_name}' already exists.")
        except ResourceNotFoundError:
            # Create the container if it does not exist
            container_client.create_container()
            print(f"Container '{container_name}' created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


def upload_to_blob(data, container_name, endpoint_name):
    try:
        # Fetch the connection string from an environment variable
        connection_string = AZURE_CONNECTION_STRING
        
        # Current date and time
        current_datetime = datetime.now()
        date_str = current_datetime.strftime("%Y-%m-%d")
        timestamp_str = current_datetime.strftime("%H%M%S")

        # Constructing the blob name
        blob_name = f"{endpoint_name}/{date_str}/{endpoint_name}.{timestamp_str}.json"

        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get a reference to the target container
        container_client = blob_service_client.get_container_client(container_name)
        
        # Create a blob client using the container and blob name
        blob_client = container_client.get_blob_client(blob_name)
        
        # Upload the data to Azure Blob Storage
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Data successfully uploaded to {container_name}/{blob_name}")
    except Exception as e:
        logging.error(f"An error occurred while uploading data to Azure Blob Storage: {e}")


# DAG definition remains the same
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'hello_spotify_dag_api',
    default_args=default_args,
    description='A simple DAG to test Spotify API',
    schedule_interval=timedelta(days=1),
)

fetch_spotify_data_task = PythonOperator(
    task_id='fetch_spotify_data_api',
    python_callable=fetch_spotify_data_api,
    op_kwargs={
        'endpoint': 'browse/new-releases'
    },
    dag=dag,
)

check_and_create_container_task = PythonOperator(
    task_id='check_and_create_container',
    python_callable=check_and_create_container,
    op_kwargs={
        'connection_string': AZURE_CONNECTION_STRING,
        'container_name': 'spotify-data'
    },
    dag=dag,
)

upload_to_blob_task = PythonOperator(
    task_id='upload_to_blob',
    python_callable=upload_to_blob,
    op_kwargs={
        'data': '{{ task_instance.xcom_pull(task_ids="fetch_spotify_data_api") }}',
        'container_name': 'spotify-data',
        'endpoint_name': 'new-releases',
        'blob_name': 'new-releases.json'
    },
    dag=dag,
)


fetch_spotify_data_task >> check_and_create_container_task >> upload_to_blob_task