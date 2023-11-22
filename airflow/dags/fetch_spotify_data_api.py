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

    results = []
    next_page_url = f"https://api.spotify.com/v1/{endpoint}"

    while next_page_url:
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        api_response = requests.get(next_page_url, headers=headers)
        # logging.info(f"Response: {api_response.text}")

        if api_response.status_code != 200:
            logging.error(f"Failed to fetch data from endpoint {endpoint}. Status Code: {api_response.status_code}")
            return results

        data = api_response.json()
        # logging.info(f"data is: {data}")
        albums = data.get('albums', {})
        results.extend(albums.get('items', []))

        # Check if there is a next page
        next_page_url = albums.get('next')

    # logging.info(f"Results type: {type(results)}") # list
    logging.info(f"Completed fetching data from {endpoint}")
    return json.dumps(results)



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

check_and_create_sql_table_task = PythonOperator(
    task_id='check_and_create_sql_table',
    python_callable=check_and_create_sql_table,
    op_kwargs={
        'sql_table_name': 'new_releases'
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

fetch_spotify_data_task >> check_and_create_container_task >> upload_to_blob_task >> check_and_create_sql_table_task >> load_data_to_sql_task >> dbt_run >> dbt_test