import sys
sys.path.append('scripts')
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from common_tasks import create_common_tasks
import json

def execute_sql_files():
    # Create a Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgresqldb')
    
    file_list_json = Variable.get("SQLFileList")
    file_list = json.loads(file_list_json)

    for file_path in file_list:
    
        # Read the SQL file
        with open(file_path, 'r') as file:
            sql = file.read()

        # Execute the SQL
        pg_hook.run(sql)

# Define default arguments
default_args = {
    'owner': 'neeraj',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule_interval='@daily',  # Schedule as needed
    start_date=days_ago(1),
    catchup=False,
)

start, end = create_common_tasks(dag)

execute_sql = PythonOperator(
        task_id='execute_sql_task',
        python_callable=execute_sql_files,
        dag = dag
)

start >> execute_sql >> end
