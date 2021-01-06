from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

"""
In below line of code we are setting ariflow configurations
"""
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)    
}

"""
Below we are creating dag
"""

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

"""
Below we are setting dummy start task
"""

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

"""
Below we are creating task to load Stage_events table
"""

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    extra_params="format as  JSON 's3://udacity-dend/log_json_path.json'"
)

"""
Below we are creating task to load Stage_songs table
"""

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    extra_params="JSON 'auto' COMPUPDATE OFF"
)

"""
Below we are creating task to load songplays table
"""

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert
)

"""
Below we are creating task to load users table
"""

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    mode='delete',
    sql_stmt=SqlQueries.user_table_insert
)

"""
Below we are creating task to load songs table
"""

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    mode='delete',
    sql_stmt=SqlQueries.song_table_insert
)

"""
Below we are creating task to load artists table
"""

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    mode='delete',
    sql_stmt=SqlQueries.artist_table_insert
)

"""
Below we are creating task to load time table
"""

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    mode='delete',
    sql_stmt=SqlQueries.time_table_insert
)

"""
Below we are defining data quality checks.
"""

dq_checks=[
{'sql_query':'select count(*) from songplays where start_time is null;','expected_result':0},
{'sql_query':'select count(distinct "level") from users;','expected_result':2},
{'sql_query':'select count(*) from staging_songs where song_id is null;','expected_result':0},
{'sql_query':'select count(*) from songs where title is null;','expected_result':0},
{'sql_query':'select count(*) from staging_songs where artist_id is null;','expected_result':0},
{'sql_query':'select count(*) from artists where name is null;','expected_result':0},
{'sql_query':'select count(*) from time where week is null;','expected_result':0}
]


"""
Below we are creating task to validate tables data
"""

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=dq_checks
)

"""
Below we are creating dummy end task 
"""

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Below we are setting Interdependencies

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]


[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table


load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]


[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks


run_quality_checks >> end_operator
