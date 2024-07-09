from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Replace with your actual values
BUCKET_NAME = 'practicedev'
DATASET_ID = 'api_ds'
TABLE_ID = 'airport_tb'
API_ENDPOINT = 'data.sfgov.org/resource/gcjv-3mzf.json'


# Define DAG
with DAG('api_to_gcs_to_bq',
         default_args=default_args,
         description='Load data from API to GCS then to BigQuery',
         schedule_interval=timedelta(hours=1),  
         start_date=datetime(2023, 6, 12),
         catchup=False) as dag:

    # Task 1: Check API availability
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='datafrom_api', 
        endpoint=API_ENDPOINT,
        method='GET',
        response_check=lambda response: True if response.status_code == 200 else False,
        poke_interval=5,
    )

    # Task 2: Extract data from API
    extract_data = SimpleHttpOperator(
        task_id='extract_data',
        http_conn_id='datafrom_api', 
        endpoint=API_ENDPOINT,
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # Task 3: Load data to GCS
    load_to_gcs = GCSToBigQueryOperator(
        task_id='load_to_gcs',
        bucket=BUCKET_NAME,
        destination_project_dataset_table=f'{DATASET_ID}.{TABLE_ID}',
        write_disposition='WRITE_APPEND',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
    )

    # Define task dependencies
    check_api >> extract_data >> load_to_gcs