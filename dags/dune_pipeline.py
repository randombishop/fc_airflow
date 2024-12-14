import airflow
from airflow import DAG
import datetime
from casts_preprocess import create_task_group as create_group1
from casts_features2 import create_task_group as create_group2
from update_sched_tasks import create_task_group as create_group3
from casts_labels import create_task_group as create_group4


default_args = {
  'start_date': airflow.utils.dates.days_ago(2),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}

with DAG(
  'dune_pipeline',
  default_args=default_args,
  description='Casts processing pipeline',
  schedule_interval='45 */2 * * *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=datetime.timedelta(hours=2)
) as dag:
  dag1 = create_group1(dag)
  dag2 = create_group2(dag)
  dag3 = create_group3(dag)
  dag4 = create_group4(dag)
  dag1 >> dag2 >> dag3 >> dag4
