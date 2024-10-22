from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'neeraj',
    'start_date': days_ago(1),
}

with DAG('parent_dag', default_args=default_args, schedule_interval='@daily') as parent_dag:
    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='child_dag',  # The DAG ID of the child DAG
        dag=parent_dag
    )

default_args = {
    'owner': 'neeraj',
    'start_date': days_ago(1),
}

with DAG('child_dag', default_args=default_args, schedule_interval='@daily') as child_dag:
    # Your tasks for the child DAG
    pass