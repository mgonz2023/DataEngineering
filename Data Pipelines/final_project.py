from datetime import datetime, timedelta
import os
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


redshift_conn_id = 'redshift'
aws_cred_id = 'aws_credentials'
dag_id = 'dag'
s3_bucket = "marcos-gonz"
s3_path_log = "s3://marcos-gonz/log_data"
s3_path_song = "s3://marcos-gonz/song_data"
region = 'us-east-1'


#Making specifications according to project guidlines
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'retries': 3, # On failure, the tasks are retried 3 times
    'retry_delay': timedelta(minutes = 5), # Retries happen every 5 minutes
    'catchup': False, # Catchup is turned off
    'email_on_retry': False # Do not email on retry
}

@dag('Final_Dag',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '0 * * * *'
    )  

def EndDag():

    start_operator = DummyOperator(task_id='Begin_execution')#,  dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id = 'Stage_events',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        aws_cred_id = aws_cred_id,
        s3_path = s3_path_log,
        region = region,
        table_name = 'staging_events',
        format = f"JSON",
        truncate = True
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id = 'Stage_songs',
        #dag=dag,
        redshift_conn_id= redshift_conn_id,
        aws_cred_id = aws_cred_id,
        s3_path = s3_path_song,
        region = region,
        table_name = 'staging_songs',
        format = f"JSON",
        truncate = True
    )

    load_songplays_table = LoadFactOperator(
        task_id = 'Load_songplays_fact_table',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        table_name = 'songplays',
        sql = final_project_sql_statements.SqlQueries.songplay_table_insert,
        truncate = True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id = 'Load_user_dim_table',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        table_name = 'users',
        sql = final_project_sql_statements.SqlQueries.user_table_insert,
        truncate = True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        table_name = 'songs',
        sql = final_project_sql_statements.SqlQueries.song_table_insert,
        truncate = True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id = 'Load_artist_dim_table',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        table_name = 'artists',
        sql = final_project_sql_statements.SqlQueries.artist_table_insert,
        truncate = True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id = 'Load_time_dim_table',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        table_name = 'time',
        sql = final_project_sql_statements.SqlQueries.time_table_insert,
        truncate = True
    )

    run_quality_checks = DataQualityOperator(
        task_id = 'Run_data_quality_checks',
        #dag=dag,
        redshift_conn_id = redshift_conn_id,
        table_list = ["songplay", "users", "song", "artist", "time"]
    )

    end_operator = DummyOperator(task_id='Stop_execution')#,  dag=dag)

    #initiate task dependencies
    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

endingDag = EndDag()
