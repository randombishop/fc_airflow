import airflow
import datetime
import os
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Import local utils
dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_folder)
from utils import exec_notebook


def notebook(**kwargs):
    ds = kwargs['ds']
    sample_size = '100000'
    params = {
        "sample_day": ds,
        "sample_size": sample_size,
        'db_host': os.environ['PG_HOST'],
        'db_database': os.environ['PG_REPLICATOR'],
        'db_user': os.environ['PG_USERNAME'],
        'db_password': os.environ['PG_PASSWORD']
    }
    task_name = 'embed'
    description = 'Airflow job executing embeddings notebook'
    gcs_notebook = 'gs://us-central1-airflow1-f888e353-bucket/notebooks/embeddings.ipynb'
    instance_type = "n1-highcpu-32"
    container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu:latest"
    kernel_spec = "python3"
    exec_notebook(task_name, description, gcs_notebook, instance_type, container_image_uri, kernel_spec, params)


default_args = {
    'start_date': airflow.utils.dates.days_ago(30),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=4)
}

with DAG(
    'embeddings',
    default_args=default_args,
    description='Calculate embeddings for casts sample',
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(hours=3),
) as dag:

    notebook_task = PythonOperator(
        task_id='notebook',
        python_callable=notebook,
        provide_context=True
    )

    notebook_task

