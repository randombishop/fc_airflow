import datetime
import airflow
from airflow import DAG
from airflow.operators.sql import SQLCheckOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator



default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'social_graph',
    default_args=default_args,
    description='Process social graph and push to Big Query',
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

    users_filename = 'pipelines/social_graph/user_names/{{ execution_date.strftime("%Y-%m-%d-%H") }}_users.csv'
    users1 = PostgresToGCSOperator(
        task_id="users1",
        postgres_conn_id='pg_replicator',
        sql='sql/user_names.sql',
        bucket='dsart_nearline1',
        filename=users_filename,
        export_format="csv",
        gzip=False
    )

    users2 = GCSToBigQueryOperator(
        task_id='users2',
        bucket='dsart_nearline1',
        source_objects=[users_filename],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_tmp.tmp_user_names',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format='CSV',
        autodetect=True
    )
    
    users3 = BigQueryInsertJobOperator(
        task_id='users3',
        configuration={
            'query': {
                'query': """
                    UPDATE `deep-mark-425321-r7.dsart_farcaster.fid_username` AS t
                    SET t.last_cast = IFNULL(TIMESTAMP_SECONDS(CAST(s.last_cast AS INT64)), t.last_cast),
                        t.num_casts = t.num_casts + s.num_casts,
                        t.user_name = IFNULL(s.user_name, t.user_name)
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_user_names` AS s
                    WHERE t.fid = s.fid ;
                    
                    INSERT INTO `deep-mark-425321-r7.dsart_farcaster.fid_username`
                    SELECT s.fid as fid,
                           TIMESTAMP_SECONDS(CAST(s.first_cast AS INT64)) as first_cast,
                           TIMESTAMP_SECONDS(CAST(s.last_cast AS INT64)) as last_cast,
                           s.num_casts as num_casts,
                           s.user_name as user_name
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_user_names` AS s
                    LEFT JOIN `deep-mark-425321-r7.dsart_farcaster.fid_username` AS t
                    ON s.fid = t.fid
                    WHERE t.fid IS NULL;
                """,
                'useLegacySql': False,
            }
        }
    )
    
    follows_filename = 'pipelines/social_graph/follows/{{ execution_date.strftime("%Y-%m-%d-%H") }}_follows.csv'
    follows1 = PostgresToGCSOperator(
        task_id="follows1",
        postgres_conn_id='pg_replicator',
        sql='sql/follows.sql',
        bucket='dsart_nearline1',
        filename=follows_filename,
        export_format="csv",
        gzip=False
    )
    
    follows2 = GCSToBigQueryOperator(
        task_id='follows2',
        bucket='dsart_nearline1',
        source_objects=[follows_filename],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_tmp.tmp_follows',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format='CSV',
        autodetect=True
    )
    
    follows3 = BigQueryInsertJobOperator(
        task_id='follows3',
        configuration={
            'query': {
                'query': """
                    UPDATE `deep-mark-425321-r7.dsart_farcaster.followers` AS t
                    SET t.added_at = COALESCE(
                                     GREATEST(TIMESTAMP_SECONDS(CAST(s.added_at AS INT64)) , t.added_at),
                                     TIMESTAMP_SECONDS(CAST(s.added_at AS INT64)),
                                     t.added_at),
                        t.removed_at = COALESCE(
                                     GREATEST(TIMESTAMP_SECONDS(CAST(s.removed_at AS INT64)) , t.removed_at),
                                     TIMESTAMP_SECONDS(CAST(s.removed_at AS INT64)),
                                     t.removed_at)
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_follows` AS s
                    WHERE t.fid_follower = s.fid_follower
                    AND t.fid_followed = s.fid_followed;
                    
                    INSERT INTO `deep-mark-425321-r7.dsart_farcaster.followers`
                    SELECT s.fid_follower,
                           s.fid_followed, 
                           TIMESTAMP_SECONDS(CAST(s.added_at AS INT64)) as added_at,
                           TIMESTAMP_SECONDS(CAST(s.removed_at AS INT64)) as removed_at
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_follows` AS s
                    LEFT JOIN `deep-mark-425321-r7.dsart_farcaster.followers` AS t
                    ON t.fid_follower = s.fid_follower AND t.fid_followed = s.fid_followed
                    WHERE t.fid_follower IS NULL AND t.fid_followed IS NULL;
                """,
                'useLegacySql': False,
            }
        }
    )
    
    reactions_filename = 'pipelines/social_graph/reactions/{{ execution_date.strftime("%Y-%m-%d-%H") }}_react.csv'
    reactions1 = PostgresToGCSOperator(
        task_id="reactions1",
        postgres_conn_id='pg_replicator',
        sql='sql/reactions.sql',
        bucket='dsart_nearline1',
        filename=reactions_filename,
        export_format="csv",
        gzip=False
    )
    
    reactions2 = GCSToBigQueryOperator(
        task_id='reactions2',
        bucket='dsart_nearline1',
        source_objects=[reactions_filename],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_tmp.tmp_reactions',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format='CSV',
        autodetect=True
    )
    
    reactions3 = BigQueryInsertJobOperator(
        task_id='reactions3',
        configuration={
            'query': {
                'query': """
                    UPDATE `deep-mark-425321-r7.dsart_farcaster.reactions` AS t
                    SET t.first_timestamp = COALESCE(
                                     LEAST(TIMESTAMP_SECONDS(CAST(s.first_timestamp AS INT64)) , t.first_timestamp),
                                     TIMESTAMP_SECONDS(CAST(s.first_timestamp AS INT64)),
                                     t.first_timestamp),
                        t.last_timestamp = COALESCE(
                                     GREATEST(TIMESTAMP_SECONDS(CAST(s.last_timestamp AS INT64)) , t.last_timestamp),
                                     TIMESTAMP_SECONDS(CAST(s.last_timestamp AS INT64)),
                                     t.last_timestamp),
                        t.num_replies = t.num_replies + s.num_replies,
                        t.num_likes = t.num_likes + s.num_likes,
                        t.num_recasts = t.num_recasts + s.num_recasts
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_reactions` AS s
                    WHERE t.fid = s.fid
                    AND t.target_fid = s.target_fid;
                    
                    INSERT INTO `deep-mark-425321-r7.dsart_farcaster.reactions`
                    SELECT s.fid,
                           TIMESTAMP_SECONDS(CAST(s.first_timestamp AS INT64)) as first_timestamp,
                           TIMESTAMP_SECONDS(CAST(s.last_timestamp AS INT64)) as last_timestamp,
                           s.target_fid, 
                           s.num_replies,
                           s.num_likes,
                           s.num_recasts
                    FROM `deep-mark-425321-r7.dsart_tmp.tmp_reactions` AS s
                    LEFT JOIN `deep-mark-425321-r7.dsart_farcaster.reactions` AS t
                    ON t.fid = s.fid AND t.target_fid = s.target_fid
                    WHERE t.fid IS NULL AND t.target_fid IS NULL;
                """,
                'useLegacySql': False,
            }
        }
    )
    
    check 
    
    check >> users1 >> users2  >> users3
    
    check >> follows1 >> follows2 >> follows3
    
    check >> reactions1 >> reactions2 >> reactions3

    

