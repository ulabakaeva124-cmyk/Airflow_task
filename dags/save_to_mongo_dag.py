from datetime import datetime
import pandas as pd
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_FILE_PATH = "/opt/airflow/dags/data/processed_data.csv"
my_dataset = Dataset(PROCESSED_FILE_PATH)


def load_data_to_mongo():
    """
    Load transformed data into MongoDB
    """
    df = pd.read_csv(PROCESSED_FILE_PATH)
    data_dict = df.to_dict('records')
    hook = MongoHook(mongo_conn_id='mongo_default')
    hook.insert_many(
        mongo_collection='reviews',
        docs=data_dict,
        mongo_db='airflow_db'
    )

with DAG('save_to_mongo_dag',
         start_date=datetime(2026, 10, 22),
         schedule=[my_dataset],
         catchup=False,
         tags=['mongo_loading'],
         ) as dag:

    load_task = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_data_to_mongo,
    )