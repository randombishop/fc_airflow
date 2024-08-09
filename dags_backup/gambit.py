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
    params = {
        "day": ds
    }
    task_name = 'gambit'
    description = 'Airflow job executing gambit notebook'
    gcs_notebook = 'gs://dsart_nearline1/notebooks/gambit.ipynb'
    instance_type = "n1-standard-4"
    container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu:latest"
    kernel_spec = "python3"
    exec_notebook(task_name, description, gcs_notebook, instance_type, container_image_uri, kernel_spec, params)


default_args = {
    'start_date': airflow.utils.dates.days_ago(32),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}

with DAG(
    'gambit',
    default_args=default_args,
    description='Run Gambit model',
    schedule_interval='0 6 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
) as dag:

    notebook_task = PythonOperator(
        task_id='gambit_notebook',
        python_callable=notebook,
        provide_context=True
    )

    notebook_task

