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
    'cast1500',
    default_args=default_args,
    schedule_interval='0 15 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    t1 = SSHOperator(
        task_id='t1',
        ssh_conn_id='ssh_caster',
		command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_misc"',
        cmd_timeout=900,
        get_pty=True)
    
    t1
    
    




