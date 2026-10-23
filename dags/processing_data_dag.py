from datetime import datetime
import os
import pandas as pd
import re
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

FILE_PATH = "/opt/airflow/dags/data/tiktok_google_play_reviews.csv"
PROCESSED_FILE_PATH = "/opt/airflow/dags/data/processed_data.csv"
my_dataset = Dataset(PROCESSED_FILE_PATH)


def replace_nulls():
    """
    Replace all nulls to '-'
    """
    df = pd.read_csv(FILE_PATH)
    df = df.fillna("-")
    df = df.astype(str)
    df = df.replace(["null", "<null>", "nan", "NaN"], "-")
    df.to_csv(FILE_PATH, index=False)


def sort_data():
    """
    Sorting data by date
    """
    df = pd.read_csv(FILE_PATH)
    df['at'] = pd.to_datetime(df['at'])
    df = df.sort_values(by='at')
    df.to_csv(FILE_PATH, index=False)


def clean_content():
    """
    Cleaning values from specific symbols and emoji
    """
    df = pd.read_csv(FILE_PATH)
    df['content'] = df['content'].astype(str).apply(
        lambda x: re.sub(r'[^\w\s\.\,\!\?\-\"]', '', x)
    )
    df.to_csv(PROCESSED_FILE_PATH, index=False)


def check_file_empty():
    """
    Checking the file size and skipping it if it's empty
    """
    if os.path.getsize(FILE_PATH) == 0:
        return 'log_empty_file'
    return 'process_data_group.replace_nulls'


with DAG(
        dag_id='process_data_dag',
        start_date=datetime(2026, 10, 22),
        schedule_interval=None,
        catchup=False,
        tags=['data_processing'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=600,
    )

    check_empty = BranchPythonOperator(
        task_id='check_empty',
        python_callable=check_file_empty,
    )


    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "The input file is empty"',
    )

    with TaskGroup("process_data_group") as process_data_group:
        task_replace = PythonOperator(
            task_id='replace_nulls',
            python_callable=replace_nulls,
        )

        task_sort = PythonOperator(
            task_id='sort_data',
            python_callable=sort_data,
        )

        task_clean = PythonOperator(
            task_id='clean_content',
            python_callable=clean_content,
            outlets=[my_dataset]
        )

        task_replace >> task_sort >> task_clean

    wait_for_file >> check_empty
    check_empty >> [log_empty_file, process_data_group]