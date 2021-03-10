from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
'''
Main program defining the Dag
'''
default_args = {
    'owner': 'capstone_cts',
    'start_date': datetime(2016,3,1),
    'end_date': datetime(2016,3,1),
    'retries': 1,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'max_active_runs': 1
    }

dag = DAG('capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *',
          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_i94vistors = StageToRedshiftOperator(
    task_id='stage_i94vistors',
    dag=dag,
    table="stage_i94visitors",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="ctsprojbucket/",
    s3_key="i94visitorstest/",
    fileformat="parquet",
    truncate_flag='Y' 
)


load_airports_dim = StageToRedshiftOperator(
    task_id='load_airports_dim',
    dag=dag,
    table="airports_dim",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="ctsprojbucket/",
    s3_key="airports/",
    fileformat="parquet",
    truncate_flag='Y'
)

load_states_dim = StageToRedshiftOperator(
    task_id='load_states_dim',
    dag=dag,
    table="states_dim",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="ctsprojbucket/",
    s3_key="states",
    fileformat="parquet",
    truncate_flag='Y'
)
load_countries_dim = StageToRedshiftOperator(
    task_id='load_countries_dim',
    dag=dag,
    table="countries_dim",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="ctsprojbucket/",
    s3_key="countries",
    fileformat="parquet",
    truncate_flag='Y'
)

Load_visitorsi94_fact = LoadFactOperator(
    task_id='Load_i94visitors_fact',
    dag=dag,
    redshift_conn_id="redshift",
    table_query=SqlQueries.visitors_fact_insert
)

load_dates_dim = LoadDimensionOperator(
    task_id='Load_dates_dim',
    dag=dag,
    redshift_conn_id="redshift",
    table_query=SqlQueries.dates_dim_insert,
    table="dates_dim",
    truncate_flag='Y'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_query="select count(1) from public.i94vistors_fact where arrivaldate is null",
    expected_count=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_i94vistors
stage_i94vistors >> load_airports_dim
stage_i94vistors >> load_countries_dim
stage_i94vistors >> load_states_dim
stage_i94vistors >> load_dates_dim
load_airports_dim >> Load_visitorsi94_fact
load_countries_dim >> Load_visitorsi94_fact
load_states_dim >> Load_visitorsi94_fact
load_dates_dim >> Load_visitorsi94_fact
Load_visitorsi94_fact >> run_quality_checks
run_quality_checks >> end_operator




