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

    df['price']=df['price'].astype('str')
    df['price']=df['price'].str.extract(r'(\d+\.\d*)')
    df['price']=df['price'].astype('float')

    df['old_price']=df['old_price'].str.extract(r'(\d+\.\d*)')
    df['old_price']=df['old_price'].astype('float')

    df.to_csv('data/laptop_list_transformed.csv', index=False, mode='w')


def fetch_data(**context):
    df=pd.read_csv('data/laptop_list_transformed.csv')
    df=df.fillna('NULL')
    # year, month, day, hour, *_ = context["data_interval_start"].timetuple()

    with open('dags/sql/write_to_db.sql', 'w') as wf:
        #laptop table
        wf.write("insert into laptop(id, name_laptop, link) values\n")
        for i in range(len(df)):
            wf.write("({}, '{}', '{}')".format(df.iloc[i]['id'], df.iloc[i]['name'], df.iloc[i]['link']))
            if i<len(df)-1:
                wf.write(",")
            wf.write("\n")
        wf.write("ON CONFLICT DO NOTHING;\n")

        #price table
        wf.write("insert into price(id_laptop, dt, price, old_price) values\n")
        for i in range(len(df)):
            wf.write("({}, '{}', {}, {})".format(df.iloc[i]['id'], context["data_interval_start"], df.iloc[i]['price'], df.iloc[i]['old_price']))
            if i<len(df)-1:
                wf.write(",")
            wf.write("\n")
        wf.write("ON CONFLICT DO NOTHING;\n")


with DAG( 
    default_args=default_args,
    dag_id='amazon_laptop_ETL',
    description='My first ETL project',
    start_date = datetime.now()- timedelta(days=2),
    schedule_interval='0 0 * * *',
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
        provide_context=True,
    )

    write_to_database_ = PostgresOperator(
        task_id='write_to_postgres',
        postgres_conn_id='my_postgres_localhost',
        sql="sql/write_to_db.sql",
    )

    crawl>>transform>>init_db>>fetch_data_>>write_to_database_
