import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
from google.cloud import bigquery

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#this dag is for fetching data from api to bigquery
# DAG definition
dag = DAG(
    'datasf_to_bigquery',
    default_args=default_args,
    description='Retrieve data from DataSF API and load to BigQuery',
    schedule_interval=timedelta(days=1),
)

# DataSF API configurations
DATASET_URL = 'https://data.sfgov.org/resource/u397-j8nr.json'
LIMIT = 1000  # Adjust the limit as per your requirement

# BigQuery configurations
PROJECT_ID = 'composer1996'
DATASET_ID = 'api_ds'
TABLE_ID = 'airport_tb'

def clean_data(records):
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            # Clean the key
            cleaned_key = key.replace(':', '').replace('@', '').replace('.', '')
            cleaned_record[cleaned_key] = value
        cleaned_records.append(cleaned_record)
    return cleaned_records

def fetch_datasf_data():
    response = requests.get(DATASET_URL, params={'$limit': LIMIT})
    response.raise_for_status()
    data = response.json()
 
    cleaned_data = clean_data(data)
    with open('/tmp/datasf_data.json', 'w') as f:
        for record in cleaned_data:
            f.write(json.dumps(record) + '\n')

def load_to_bigquery():
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open('/tmp/datasf_data.json', 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    
    job.result()  # Wait for the job to complete.

fetch_data_task = PythonOperator(
    task_id='fetch_datasf_data',
    python_callable=fetch_datasf_data,
    dag=dag,
)

load_to_bq_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

fetch_data_task >> load_to_bq_task
