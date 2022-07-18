from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from taxis_etl import (
    create_schema,
    create_load_borough,
    create_load_payment,
    create_load_rate,
    create_load_vendor,
    create_load_location,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,1,1,0,0,0,0),
    'email': ['walvdev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

dag_schema = DAG(
    'DB_Cration',
    default_args=default_args,
    description='Create Data Base Schema and load some dimention Tables (No Mutables)',
    schedule_interval="@once",
    catchup=False,
    tags=['Create','Load'],
)

task_create_schema = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    dag=dag_schema,
)

task_borough = PythonOperator(
    task_id='load_borough',
    python_callable=create_load_borough,
    dag=dag_schema,
)

task_payment = PythonOperator(
    task_id='load_payment',
    python_callable=create_load_payment,
    dag=dag_schema,
)

task_rate = PythonOperator(
    task_id='load_rate',
    python_callable=create_load_rate,
    dag=dag_schema,
)

task_vendor = PythonOperator(
    task_id='load_vendor',
    python_callable=create_load_vendor,
    dag=dag_schema,
)

task_location = PythonOperator(
    task_id='load_location',
    python_callable=create_load_location,
    op_kwargs={"file":f"{dag_schema.folder}/../Data/taxi+_zone_lookup.csv"},
    dag=dag_schema,
)

task_create_schema >> [task_vendor,task_rate,task_payment,task_borough]
task_borough >> task_location