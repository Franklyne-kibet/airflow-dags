import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

BUCKET = os.getenv('GCP_GCS_BUCKET')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', "trips_data_all")


dataset_file = "yellow_tripdata_2021-01.csv.gz"
dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = AIRFLOW_HOME
parquet_file = dataset_file.replace(".csv.gz", ".parquet")


#format to parquet
def format_to_parquet(src_file):
    if not src_file.endswith('.csv.gz'):
        logging.error('Can only accept csv file format at the moment')
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))

#upload to gcs
def local_to_gcs(bucket,object_name, local_file):

    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args ={
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

#Declaring DAG implicit way
with DAG(
    dag_id = "data_ingestion_gcs_dag",
    schedule='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['ny-rides'],
)as dag:
    
    download_dataset_task = BashOperator(
        task_id ="Download_dataset",
        bash_command = f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parqute_task = PythonOperator(
        task_id = "Format_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "src_file": f"{path_to_local_home}/{dataset_file}"
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id = "local_to_gcs",
        python_callable = local_to_gcs,
        op_kwargs = {
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file":f"{path_to_local_home}/{parquet_file}"
        }
    )

    big_query_external_table_task = BigQueryCreateExternalTableOperator(
        task_id = "create_bigquery_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parqute_task >> local_to_gcs_task >> big_query_external_table_task
