from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


def print_result():
    print('thanh cong')


with DAG( 
    default_args=default_args,
    dag_id='amazon_laptop_ETL',
    description='Our first ETL project',
    start_date=datetime(2023, 2, 20),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='crawl_laptop_list',
        bash_command="scripts/run_laptop_list_optional_len.sh",
        # bash_command='pwd',
        # bash_command="echo ahaha",
    )

    task2 = PythonOperator(
        task_id='print_result',
        python_callable=print_result,
    )

    task1>>task2
