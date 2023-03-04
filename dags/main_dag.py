from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd


default_args = {
    'owner': 'hieu_nguyen',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


def transform_data():
    df=pd.read_csv('data/laptop_list.csv')
    df['id']=df['name'].apply(lambda x: hash(x)%1000000)
    df.to_csv('data/laptop_list_transformed.csv', index=False, mode='w')


def fetch_data():
    df=pd.read_csv('data/laptop_list_transformed.csv')
    values=[]

    def abc(row):
        values.append([row['id'], row['name'], row['link']])
    df.apply(lambda x: abc(x), axis=1)
    
    with open('dags/sql/write_to_db.sql', 'w') as wf:
        wf.write("insert into laptop(id, name_laptop, link) values\n")
        for i, val in enumerate(values):
            wf.write("({}, '{}', '{}')".format(val[0], val[1], val[2]))
            if i<len(values)-1:
                wf.write(",")
            wf.write("\n")
        wf.write("ON CONFLICT DO NOTHING")
    

with DAG( 
    default_args=default_args,
    dag_id='amazon_laptop_ETL1',
    description='My first ETL project',
    start_date = datetime.now()- timedelta(days=2),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    crawl = BashOperator(
        task_id='crawl_laptop_list',
        bash_command="scripts/run_laptop_list_optional_len.sh",
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    init_db = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='my_postgres_localhost',
        sql="sql/init_db.sql",
    )

    fetch_data_=PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
    )

    write_to_database_ = PostgresOperator(
        task_id='write_to_postgres',
        postgres_conn_id='my_postgres_localhost',
        sql="sql/write_to_db.sql",
    )

    crawl>>transform>>init_db>>fetch_data_>>write_to_database_
