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
    'cast1630',
    default_args=default_args,
    schedule_interval='30 16 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    cast_arts = SSHOperator(
        task_id='cast_arts',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_arts"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_culture = SSHOperator(
        task_id='cast_culture',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_culture"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_arts >> cast_culture
    
    




