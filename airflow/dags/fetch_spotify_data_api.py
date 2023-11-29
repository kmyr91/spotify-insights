from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
import json
import requests
import logging
import base64
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from airflow.configuration import conf

from sqlalchemy import create_engine
from sqlalchemy.sql import text 



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
        return

    response_data = response.json()

    if 'access_token' not in response_data:
        logging.error("Access token not found in the response.")
        return

    access_token = response_data['access_token']

    # Case 1: Fetch new releases
    
    new_releases_url = f"https://api.spotify.com/v1/{endpoint}"
    new_releases_response = requests.get(new_releases_url, headers={"Authorization": f"Bearer {access_token}"})
    if new_releases_response.status_code != 200:
        logging.error(f"Failed to fetch new releases. Status Code: {new_releases_response.status_code}")
            

        new_releases_data = new_releases_response.json()
        albums = new_releases_data.get('albums', {}).get('items', [])
        artist_ids = set(album['artists'][0]['id'] for album in albums if album.get('artists'))
        
        # Push artist IDs to XCom
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='artist_ids', value=list(artist_ids))
        return json.dumps(albums)


def fetch_artists_top_tracks(endpoint, **kwargs):
    
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
        return

    response_data = response.json()

    if 'access_token' not in response_data:
        logging.error("Access token not found in the response.")
        return

    access_token = response_data['access_token']
    
    
    task_instance = kwargs.get('ti')
    artist_ids = task_instance.xcom_pull(task_ids='fetch_spotify_data_api', key='artist_ids')
    market='ES'
    if not artist_ids:
        logging.error("No artist IDs found.")
        return
   
    # Fetch new releases to get artist IDs
    new_releases_response = requests.get("https://api.spotify.com/v1/browse/new-releases", headers={"Authorization": f"Bearer {access_token}"})
    new_releases_data = new_releases_response.json()
    albums = new_releases_data.get('albums', {}).get('items', [])
        

    # Fetch top tracks for each artist
    artist_top_tracks = []
    for artist_id in artist_ids:
        top_tracks_url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=US"
        top_tracks_response = requests.get(top_tracks_url, headers={"Authorization": f"Bearer {access_token}"})
        if top_tracks_response.status_code == 200:
            top_tracks_data = top_tracks_response.json().get('tracks', [])
            artist_top_tracks.extend(top_tracks_data)
        else:
            logging.error(f"Failed to fetch top tracks for artist {artist_id}. Status Code: {top_tracks_response.status_code}")

    return json.dumps(artist_top_tracks)

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

        blob_name = f"{endpoint_name}/{date_str}/{endpoint_name}.{timestamp_str}.json"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        # Convert to JSON, then to bytes
        json_data = json.loads(data)
        json_string = json.dumps(json_data)
        data_bytes = json_string.encode('utf-8')
        
        # Upload the data to Azure Blob Storage
        blob_client.upload_blob(data_bytes, overwrite=True)
        logging.info(f"Data successfully uploaded to {container_name}/{blob_name}")
        return blob_name
    except Exception as e:
        logging.error(f"An error occurred while uploading data to Azure Blob Storage: {e}")

def check_and_create_sql_table(sql_table_name):
    # Connect to Azure SQL Database
    sql_conn_str = conf.get('azure', 'sql_connection_string')
    engine = create_engine(sql_conn_str) 

    try: 
        with engine.connect() as connection:
            # Check if the table exists
            try:
                connection.execute(f"SELECT TOP 1 * FROM {sql_table_name}")
                logging.info(f"Table '{sql_table_name}' already exists.")
            except Exception as e:
                logging.info(f"Table '{sql_table_name}' does not exist. Creating table...")
                create_table_query = text(f"CREATE TABLE {sql_table_name} (id INT IDENTITY(1,1) PRIMARY KEY, json_column NVARCHAR(MAX) NOT NULL)")
                connection.execute(create_table_query)
                logging.info(f"Table '{sql_table_name}' created successfully.")
    except Exception as e:
        logging.error(f"An error occurred while connecting to Azure SQL Database: {e}")

def load_data_to_sql(container_name, blob_name, sql_table_name):
    # Connect to Blob Storage and retrieve data
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    json_data = blob_client.download_blob().readall()

    # Parse JSON data
    data_objects = json.loads(json_data.decode('utf-8'))

    # Connect to Azure SQL Database
    sql_conn_str = conf.get('azure', 'sql_connection_string')
    engine = create_engine(sql_conn_str) 

    try: 
        with engine.connect() as connection:
            for data_object in data_objects:
                # Ensure data_object is a dictionary
                if isinstance(data_object, dict):
                    obj_json_data = json.dumps(data_object)
                    insert_query = text(f"INSERT INTO {sql_table_name} (json_column) VALUES (:json_data)")
                    connection.execute(insert_query, json_data=obj_json_data)
                else:
                    logging.error("Data object is not a dictionary")
    except Exception as e:
        logging.error(f"An error occurred while loading data to Azure SQL Database: {e}")



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
    provide_context=True,  # Important to enable XComs
    dag=dag,
)

# Task for fetching top tracks
fetch_artists_top_tracks_task = PythonOperator(
    task_id='fetch_artists_top_tracks',
    python_callable=fetch_artists_top_tracks,
    op_kwargs={
        'endpoint': 'artist_top_tracks'
    },
    provide_context=True,  # Important to enable XComs

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

# Task for uploading top tracks data to Blob
upload_top_tracks_to_blob_task = PythonOperator(
    task_id='upload_top_tracks_to_blob',
    python_callable=upload_to_blob,
    op_kwargs={
        'data': '{{ task_instance.xcom_pull(task_ids="fetch_artists_top_tracks") }}',  # Ensure this task_id matches your fetch task
        'container_name': 'spotify-data',
        'endpoint_name': 'top-tracks',
        'blob_name': 'top-tracks.json'  # This blob name can be set dynamically in your function
    },
    dag=dag,
)

load_data_to_sql_task = PythonOperator(
    task_id='load_data_to_sql',
    python_callable=load_data_to_sql,
    op_kwargs={
        'container_name': 'spotify-data',
        'blob_name': '{{ task_instance.xcom_pull(task_ids="upload_to_blob") }}',
        'sql_table_name': 'new_releases'
    },
    dag=dag,
)

# Task for loading top tracks data to SQL
load_top_tracks_to_sql_task = PythonOperator(
    task_id='load_top_tracks_to_sql',
    python_callable=load_data_to_sql,
    op_kwargs={
        'container_name': 'spotify-data',
        'blob_name': '{{ task_instance.xcom_pull(task_ids="upload_top_tracks_to_blob") }}',
        'sql_table_name': 'top_tracks'
    },
    dag=dag,
)

check_and_create_sql_table_task = PythonOperator(
    task_id='check_and_create_sql_table',
    python_callable=check_and_create_sql_table,
    op_kwargs={
        'sql_table_name': 'new_releases'
    },
    dag=dag,
)

check_and_create_top_tracks_table_task = PythonOperator(
    task_id='check_and_create_top_tracks_table',
    python_callable=check_and_create_sql_table,
    op_kwargs={
        'sql_table_name': 'top_tracks'
    },
    dag=dag,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt/dbt_spotify && dbt run --target=dev --models=new_releases',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt/dbt_spotify && dbt test --target=dev --models=new_releases',
    dag=dag,
)



# Define task dependencies
#fetch_spotify_data_task >> check_and_create_container_task
#fetch_artists_top_tracks_task >> check_and_create_container_task
#check_and_create_container_task >> upload_to_blob_task >> check_and_create_sql_table_task >> load_data_to_sql_task >> dbt_run >> dbt_test

# New Releases Data Flow
fetch_spotify_data_task >> check_and_create_container_task >> upload_to_blob_task >> check_and_create_sql_table_task >> load_data_to_sql_task

# Top Tracks Data Flow (dependent on fetch_spotify_data_task via XCom)
fetch_artists_top_tracks_task >> upload_top_tracks_to_blob_task >> check_and_create_top_tracks_table_task >> load_top_tracks_to_sql_task

# DBT tasks should start after both data flows have been completed
[load_data_to_sql_task, load_top_tracks_to_sql_task] >> dbt_run >> dbt_test

