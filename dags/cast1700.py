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
    'cast1700',
    default_args=default_args,
    schedule_interval='00 17 * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    cast_business = SSHOperator(
        task_id='cast_business',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_business"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_tech_science = SSHOperator(
        task_id='cast_tech_science',
        ssh_conn_id='ssh_caster',
        command='/home/na/.bun/bin/bun fc_caster/app/index.ts digest "{{ ds }}" "c_tech_science"',
        cmd_timeout=900,
        get_pty=True)
    
    cast_business >> cast_tech_science
    
    



