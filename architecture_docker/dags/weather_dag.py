from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from taxis_etl import (
    extract_transform_weather,
    load_weather,
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2018,1,1,0,0,0,0),
    'email': ['walvdev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(hours=24)
}

dag_weather = DAG(
    'ETL_Weather',
    default_args=default_args,
    description='Call API weather to get weather data and load in tha database',
    schedule_interval="@monthly",
    max_active_runs=5,
    catchup=True,
    tags=['Extract','Transfrom','Load'],
)

task_extract_transform_weather = PythonOperator(
    task_id='extract_transform_weather',
    python_callable=extract_transform_weather,
    op_kwargs={
        "year":"{{data_interval_start.year}}",
        "month":"{{data_interval_start.month}}",
        "out_dir":f"{dag_weather.folder}/../Data/"
    },
    dag=dag_weather,
)

task_load_weather = PythonOperator(
    task_id='load_weather',
    python_callable=load_weather,
    op_kwargs={
        "file":f'{dag_weather.folder}/../Data/Weather_{"{{data_interval_start.year}}"}_{"{{data_interval_start.month}}"}.csv'},
    dag=dag_weather,
)

task_extract_transform_weather >> task_load_weather
