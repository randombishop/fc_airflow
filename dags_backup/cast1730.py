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
    'cast1730',
    default_args=default_args,
    schedule_interval='30 17 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    cast_money = SSHOperator(
        task_id='cast_money',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_money"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_sports = SSHOperator(
        task_id='cast_sports',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_sports"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_nature = SSHOperator(
        task_id='cast_nature',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_nature"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_money >> cast_sports >> cast_nature
    
    



