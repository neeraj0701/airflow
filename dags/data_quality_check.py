import sys
sys.path.append('scripts')
from airflow import DAG
from airflow.utils.dates import days_ago
from my_custom_operator import DataQualityCheckOperator  # Adjust the import as needed
from common_tasks import create_common_tasks

default_args = {
    'owner': 'neeraj',
    'start_date': days_ago(1),
    'catchup': False
}

dag = DAG('data_quality_check', default_args=default_args, schedule_interval='@daily')

start, end = create_common_tasks(dag)

data_quality_task = DataQualityCheckOperator(
    task_id='check_data_quality',
    table='sales_raw',
    column='sale_amount',
    postgres_conn_id='postgresqldb',  # Your connection ID
    dag=dag,
)

start >> data_quality_task >> end
