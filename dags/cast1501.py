import airflow
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import timedelta


default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with DAG(
    'cast1501',
    default_args=default_args,
    schedule_interval='01 15 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    cast_misc = SSHOperator(
        task_id='cast_misc',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_misc"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_misc
    
    




