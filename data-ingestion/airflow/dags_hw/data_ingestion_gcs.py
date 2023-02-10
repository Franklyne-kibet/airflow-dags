import os
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID","ny-rides-franklyne")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "prefect-data-zoomcamp")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv.gz'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def donwload_parquetize_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

URL_PREFIX_FHV = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv'

FHV_TAXI_URL_TEMPLATE = URL_PREFIX_FHV + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
FHV_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
FHV_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_GCS_PATH_TEMPLATE = "data/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data_v00",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    dagrun_timeout=timedelta(seconds=2000),
    catchup=True,
    max_active_runs=3,
    tags=['ny-rides'],
)

donwload_parquetize_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_TAXI_URL_TEMPLATE,
    local_csv_path_template=FHV_TAXI_CSV_FILE_TEMPLATE,
    local_parquet_path_template=FHV_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=FHV_TAXI_GCS_PATH_TEMPLATE
)