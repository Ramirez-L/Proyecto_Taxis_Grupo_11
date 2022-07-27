from datetime import datetime, timedelta
from statistics import mode

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from taxis_etl import (
    extract_trip,
    transform_trip,
    load_trip,
)
from cfg import (
    BATCH_SIZE_TRIP,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2018,1,1,0,0,0,0),
    'email': ['walvdev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

dag_taxis_trip = DAG(
    'ETL_taxi_trips',
    default_args=default_args,
    description='ETL for trips of yellow taxis of New york',
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=['Extract','Transfrom','Load'],
)

def _task_extract_trip(year: str,month: str,out_dir:str,ti):
    if len(month)==1:
        month="0"+month
    parquet_file=extract_trip(year,month,out_dir)
    ti.xcom_push(key='parquet_file', value=parquet_file)
    print("push",parquet_file,"*"*500)

def _task_transform_trip(year: str,month: str,out_dir:str,ti):
    from pyarrow.parquet import ParquetFile
    import pyarrow as pa 
    import os 
    
    if len(month)==1:
        month="0"+month

    parquet_file=ti.xcom_pull(key='parquet_file',task_ids="extract_trip")
    csv_clean_file=f"clean_tripdata_{year}-{month}.csv"

    header=["id_vendor","pickup_datetime","dropoff_datetime","passager_count","trip_distance","id_rate",
    "store_fwd_flag","pu_location","do_location","id_payment","fare_amount","extra","mta_tax","tip_amount","tolls_amount",
    "improve_surcharge","total_amount","congestion_surcharge","tiempo_viaje"]
    
    with open(out_dir+csv_clean_file,"w+") as f:
        f.write(','.join(header)+'\n')
    ti.xcom_push(key='csv_clean_file', value=csv_clean_file)

    pf_taxis = ParquetFile(out_dir+parquet_file)
    for i,batch in enumerate(pf_taxis.iter_batches(batch_size=BATCH_SIZE_TRIP)):
        print("Transform Batch",i,"."*50)
        data = transform_trip(batch.to_pandas(),year,month)
        data.to_csv(out_dir+csv_clean_file, index=False,header=False,mode="a+")

    if os.path.exists(out_dir+parquet_file):
        os.remove(out_dir+parquet_file)


def _task_load_trip(out_dir:str,ti):
    import pandas as pd
    import os

    csv_clean_file=ti.xcom_pull(key='csv_clean_file',task_ids="transform_trip")

    with pd.read_csv(out_dir+csv_clean_file, chunksize=BATCH_SIZE_TRIP) as reader:
        for i, batch in enumerate(reader):
            print("Loading Batch",i,"."*50)
            load_trip(batch)
    if os.path.exists(out_dir+csv_clean_file):
        os.remove(out_dir+csv_clean_file)

task_extract_trip = PythonOperator(
    task_id='extract_trip',
    python_callable=_task_extract_trip,
    op_kwargs={
        "year":"{{data_interval_start.year}}",
        "month":"{{data_interval_start.month}}",
        "out_dir":f"{dag_taxis_trip.folder}/../Data/" 
    },
    dag=dag_taxis_trip,
)

task_transform_trip = PythonOperator(
    task_id='transform_trip',
    python_callable=_task_transform_trip,
    op_kwargs={
        "year":"{{data_interval_start.year}}",
        "month":"{{data_interval_start.month}}",
        "out_dir":f"{dag_taxis_trip.folder}/../Data/" 
    },
    dag=dag_taxis_trip,
)

task_load_trip = PythonOperator(
    task_id='load_trip',
    python_callable=_task_load_trip,
    op_kwargs={
        "out_dir":f"{dag_taxis_trip.folder}/../Data/" 
    },
    dag=dag_taxis_trip,
)

task_extract_trip >> task_transform_trip >> task_load_trip
