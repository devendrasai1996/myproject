from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.cloud import bigquery
import json
import tempfile
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'Devendra',
    'depends_on_past': False,
    'email': ['devendrasaikowtarapu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to infer schema from data
def infer_schema(data):
    # Create a BigQuery client
    client = bigquery.Client()

    # Use the BigQuery schema inference feature
    table = bigquery.Table('mypractice-424104.api_ds.airport_tb')
    schema = client.schema_from_json(data[0])
    
    return schema

# Function to transform data and infer schema
def transform_and_infer_schema(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    # Transform data if needed
    transformed_data = data  # Placeholder for actual transformation logic
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as tmpfile:
        json.dump(transformed_data, tmpfile)
        return tmpfile.name, infer_schema(transformed_data)

# Function to upload data to GCS
def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    return f'gs://{bucket_name}/{destination_blob_name}'

# Define the DAG
with DAG(
    'api_to_bigquery',
    default_args=default_args,
    description='Load data from API to BigQuery',
    schedule_interval=timedelta(hours=1),  # Adjust frequency as needed
    start_date=datetime(2024, 6, 12),
    catchup=False,
) as dag:

    # 1. Check if the API is available
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='datafrom_api',  # Replace with your Airflow connection ID
        endpoint='resource/gcjv-3mzf.json',  # Replace with your API endpoint
        method='GET',
        response_check=lambda response: True if response.status_code == 200 else False,
    )

    # 2. Extract data from the API
    extract_data = HttpOperator(
        task_id='extract_data',
        http_conn_id='datafrom_api',
        endpoint='resource/gcjv-3mzf.json',
        method='GET',
        response_filter=lambda response: json.loads(response.text),  # Assuming JSON response
        log_response=True,
    )

    # 3. Create BigQuery Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='api_ds',
        project_id='mypractice-424104',  # Replace with your project ID
    )

    # 4. Transform data and infer schema
    transform_and_infer_schema = PythonOperator(
        task_id='transform_and_infer_schema',
        python_callable=transform_and_infer_schema,
        provide_context=True,
    )

    # 5. Create BigQuery Table with inferred schema
    def create_table_with_schema(**kwargs):
        ti = kwargs['ti']
        _, schema = ti.xcom_pull(task_ids='transform_and_infer_schema')
        client = bigquery.Client(project='mypractice-424104')
        table_id = 'mypractice-424104.api_ds.airport_tb'
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        return table

    create_table = PythonOperator(
        task_id='create_table_with_schema',
        python_callable=create_table_with_schema,
        provide_context=True,
    )

    # 6. Upload transformed data to GCS
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'file_path': '{{ ti.xcom_pull(task_ids="transform_and_infer_schema")[0] }}',
            'bucket_name': 'your-gcs-bucket-name',  # Replace with your GCS bucket name
            'destination_blob_name': 'data/airport_data.json'  # Replace with your GCS object name
        },
    )

    # 7. Load data into BigQuery
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            'jobType': 'LOAD',
            'load': {
                'destinationTable': {
                    'projectId': 'mypractice-424104',  
                    'datasetId': 'api_ds',
                    'tableId': 'airport_tb'
                },
                'sourceUris': ['gs://your-gcs-bucket-name/data/airport_data.json'],  # Replace with your GCS object URI
                'sourceFormat': 'NEWLINE_DELIMITED_JSON',
                'writeDisposition': 'WRITE_APPEND',  # Change to 'WRITE_TRUNCATE' to overwrite table
                'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION'],  # Allow adding new columns
            }
        },
    )

    # Set task dependencies
    check_api >> extract_data >> create_dataset >> transform_and_infer_schema >> create_table >> upload_to_gcs_task >> load_to_bigquery
