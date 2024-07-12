import airflow
import datetime
import os
import sys
import pandas
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.hooks.postgres_hook import PostgresHook


# Import local utils
dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_folder)
from utils import exec_notebook


def notebook_digest(**kwargs):
    ds = kwargs['ds']
    params = {
        "sample_day": ds
    }
    task_name = 'digest'
    description = 'Airflow job executing digest notebook'
    gcs_notebook = 'gs://dsart_nearline1/notebooks/digest.ipynb'
    instance_type = "n1-standard-4"
    container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu:latest"
    kernel_spec = "python3"
    exec_notebook(task_name, description, gcs_notebook, instance_type, container_image_uri, kernel_spec, params)

def csv_to_postgres(**kwargs):
    ds = kwargs['ds']
    tmp_file = '/tmp/' + ds + '_digest.json'
    pg_hook = PostgresHook(postgres_conn_id='pg_replicator')
    json_data = json.load(open(tmp_file))
    df = pandas.DataFrame(json_data)
    print('rows', len(df))
    df['day'] = ds
    columns = ['day', 'key', 'title', 'summary', 'links']
    df = df[columns]
    print(df)
    res = df.to_sql(name='daily_digest', 
           con=pg_hook.get_sqlalchemy_engine(),
           schema='ds', 
           if_exists='append', 
           index=False, 
           chunksize=256)
    print('Inserted rows', res)
    os.remove(tmp_file)
    print('Removed file', tmp_file)

default_args = {
    'start_date': airflow.utils.dates.days_ago(10),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}

with DAG(
    'digest',
    default_args=default_args,
    description='Generate daily digests',
    schedule_interval='0 14 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=30),
) as dag:

    task_vertex = PythonOperator(
        task_id='vertex',
        python_callable=notebook_digest,
        provide_context=True
    )

    task_tmp = GCSToLocalFilesystemOperator(
        task_id='tmp',
        bucket='dsart_nearline1',
        object_name='pipelines/digest/{{ ds }}.json',
        filename='/tmp/{{ ds }}_digest.json'
    )

    task_pg = PythonOperator(
        task_id='pg',
        python_callable=csv_to_postgres,
        provide_context=True
    )

    task_vertex >> task_tmp >> task_pg

