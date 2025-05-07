from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from datetime import datetime

import os
from airflow.decorators import task
from airflow.decorators import dag, task

from src.utils import sample_utility_function, fetch_data, preprocess_data



@dag(dag_id='simple_ml_pipeline', start_date=datetime(2024, 4, 4), schedule_interval=None,catchup=False)
## Sample DAG using python virtual env operator
## Important to note that virtual env doesnt support xcom push and pull
## https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html#id1
def bitcoin_model():
    @task.virtualenv(
    task_id="fetch_data", requirements=["scikit-learn", "pandas"], system_site_packages=False
    )
    def fetch_data_task():
        import sys
        CUSTOM_LIB_PATH = "./src"
        sys.path.insert(0, CUSTOM_LIB_PATH)
        from utils import fetch_data 
        return fetch_data()
    
    @task.virtualenv(
    task_id="preprocess_data", requirements=["scikit-learn", "pandas"], system_site_packages=False
    )
    def preprocess_data_task(data_path):
        return preprocess_data(data_path=data_path)

    dest_file = fetch_data_task()
    dest_file = preprocess_data_task(data_path=dest_file)


bitcoin_model()