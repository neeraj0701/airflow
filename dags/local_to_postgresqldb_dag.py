import pandas as pd    
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

def load_data_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgresqldb')
    
    df = pd.read_csv(r'/opt/airflow/files/Customers.csv')

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        cursor.execute("""
            TRUNCATE TABLE Customers;
            INSERT INTO Customers (CustomerID, CustomerName, City)
            VALUES (%s, %s, %s);
        """, (row['CustomerID'], row['CustomerName'], row['City']))

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'load_data_to_postgresql',
    default_args={'owner': 'neeraj', 'start_date': days_ago(1)},
    schedule_interval=None,
) as dag:

    start = EmptyOperator(
        task_id='start'
    )
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_postgres,
    )
    
    end = EmptyOperator(
        task_id='end'
    )

    start >> load_data_task >> end