import datetime
import airflow
from airflow import DAG
from airflow.operators.sql import SQLCheckOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator



default_args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'process_casts',
    default_args=default_args,
    description='Process casts hourly from postgres to BigQuery',
    schedule_interval='0 * * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(hours=3)
) as dag:

    check = SQLCheckOperator(
        task_id='check',
        conn_id='pg_replicator',
		sql='sql/check_casts.sql')
    
    check

    snapshot_casts = PostgresToGCSOperator(
        task_id="snapshot_casts",
        postgres_conn_id='pg_replicator',
        sql='sql/snapshot_casts.sql',
        bucket='dsart_nearline1',
        filename='pipelines/process_casts/casts/{{ execution_date.strftime("%Y-%m-%d-%H") }}_casts.csv',
        export_format="csv",
        gzip=False
    )

    embeddings = SSHOperator(
        task_id='embeddings',
        ssh_conn_id='ssh_worker',
        command='/home/na/embeddings.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
        cmd_timeout=300,
        get_pty=True)
    
    gambit = SSHOperator(
        task_id='gambit',
        ssh_conn_id='ssh_worker',
        command='/home/na/gambit2.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
        cmd_timeout=300,
        get_pty=True)
    
    user_stats = PostgresToGCSOperator(
        task_id="user_stats",
        postgres_conn_id='pg_replicator',
        sql='sql/user_stats.sql',
        bucket='dsart_nearline1',
        filename='pipelines/process_casts/users/{{ execution_date.strftime("%Y-%m-%d-%H") }}_users.csv',
        export_format="csv",
        gzip=False
    )

    join = SSHOperator(
        task_id='join',
        ssh_conn_id='ssh_worker',
        command='/home/na/join1.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
        cmd_timeout=300,
        get_pty=True)
    
    bird = SSHOperator(
        task_id='bird',
        ssh_conn_id='ssh_worker',
        command='/home/na/bird2.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
        cmd_timeout=300,
        get_pty=True)
    
    bird_filename = 'pipelines/process_casts/bird/{{ execution_date.strftime("%Y-%m-%d-%H") }}_bird.csv'
    bird_bq = GCSToBigQueryOperator(
        task_id='bird_bq',
        bucket='dsart_nearline1',
        source_objects=[bird_filename],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_tmp.tmp_bird',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format='CSV',
        autodetect=True
    )
    
    bird_update = BigQueryInsertJobOperator(
        task_id='bird_update',
        configuration={
            'query': {
                'query': """
                    UPDATE `deep-mark-425321-r7.dsart_farcaster.cast_features` AS t
                    SET t.predict_like = CAST((100*s.predict_like) as INT64)
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_bird` AS s
                    WHERE t.day = '{{ execution_date.strftime("%Y-%m-%d") }}' 
                    AND t.hash = s.hash
                """,
                'useLegacySql': False,
            }
        }
    )
    
    check 
    
    check >> snapshot_casts >> embeddings >> gambit

    check >> user_stats

    (gambit, user_stats) >> join >> bird >> bird_bq >> bird_update



