from airflow import DAG
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'neeraj',
    'retries': 1,
}
    
def dbt_hook_func():
    dbt_hook = DbtCloudHook(dbt_cloud_conn_id='dbtcloudconn')
    dbt_hook.trigger_job_run(job_id = Variable.get("JobId"),cause = 'to test')
        
with DAG(
    'trigger_dbt_cloud_job',
    default_args=default_args,
    description='Trigger a dbt Cloud job',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt', 'trigger'],
) as dag:
    
    start = EmptyOperator(
        task_id='Start',
    )
    
    trigger_job = PythonOperator(
        task_id='trigger_dbt_job',
        python_callable=dbt_hook_func,
    )
    
    end = EmptyOperator(
        task_id='End'       
    )
        
    start >> trigger_job >> end