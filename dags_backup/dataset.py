import airflow
import datetime
import os
import sys
import pandas
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator



# Import local utils
dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_folder)
from utils import exec_notebook


def notebook1(**kwargs):
    ds = kwargs['ds']
    params = {
        "sample_day": ds
    }
    task_name = 'dataset1'
    description = 'Airflow job executing dataset1 notebook'
    gcs_notebook = 'gs://dsart_nearline1/notebooks/dataset1.ipynb'
    instance_type = "n1-standard-4"
    container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu:latest"
    kernel_spec = "python3"
    exec_notebook(task_name, description, gcs_notebook, instance_type, container_image_uri, kernel_spec, params)

def notebook_bird(**kwargs):
    ds = kwargs['ds']
    params = {
        "sample_day": ds
    }
    task_name = 'bird'
    description = 'Airflow job executing bird notebook'
    gcs_notebook = 'gs://dsart_nearline1/notebooks/bird.ipynb'
    instance_type = "n1-standard-4"
    container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu:latest"
    kernel_spec = "python3"
    exec_notebook(task_name, description, gcs_notebook, instance_type, container_image_uri, kernel_spec, params)

def csv_to_postgres(**kwargs):
    ds = kwargs['ds']
    tmp_file = '/tmp/' + ds + '_bird.csv'
    pg_hook = PostgresHook(postgres_conn_id='pg_replicator')
    df = pandas.read_csv(tmp_file, lineterminator='\n')
    print('CSV rows', len(df))
    res = df.to_sql(name='bird1', 
           con=pg_hook.get_sqlalchemy_engine(),
           schema='ds', 
           if_exists='append', 
           index=False, 
           chunksize=256)
    print('Inserted rows', res)
    os.remove(tmp_file)
    print('Removed file', tmp_file)

default_args = {
    'start_date': airflow.utils.dates.days_ago(35),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}

with DAG(
    'dataset',
    default_args=default_args,
    description='Create casts dataset',
    schedule_interval='0 12 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=30),
) as dag:

    task1 = PythonOperator(
        task_id='ds1',
        python_callable=notebook1,
        provide_context=True
    )

    task_bird = PythonOperator(
        task_id='bird',
        python_callable=notebook_bird,
        provide_context=True
    )

    task_tmp_file = GCSToLocalFilesystemOperator(
        task_id='tmp_file',
        bucket='dsart_nearline1',
        object_name='pipelines/bird1/{{ ds }}.csv',
        filename='/tmp/{{ ds }}_bird.csv'
    )

    task_bird_pg = PythonOperator(
        task_id='bird_pg',
        python_callable=csv_to_postgres,
        provide_context=True
    )

    task_bird_stats = PostgresOperator(
        task_id='bird_stats',
        postgres_conn_id='pg_replicator',
        sql='sql/bird1_stats.sql'
    )    

    task1 >> task_bird >> task_tmp_file >> task_bird_pg >> task_bird_stats
