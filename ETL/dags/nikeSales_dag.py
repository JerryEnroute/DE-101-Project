# Importing necessary libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sales_scripts import get_data, transform_data, load_data
import csv

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Initialize the DAG
with DAG('nikeSales_dag',
        default_args=default_args,
        schedule_interval=None,
        concurrency=3,
        ) as dag:
    extract = PythonOperator(
        task_id='extract_task',
        python_callable=get_data.main,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data.main,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load_task',
        python_callable=load_data.main,
        provide_context=True,
    )

    # noinspection PyStatementEffect
    extract >> transform >> load
