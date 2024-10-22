import sys
sys.path.append('scripts')
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from common_tasks import create_common_tasks

# Default arguments for the DAG
default_args = {
    'owner': 'neeraj',
    'start_date': days_ago(1),
    'retries': 1,
}

# Function to generate a random number
def generate_random_number(**kwargs):
    random_number = random.randint(1, 100)  # Generate a random number between 1 and 100
    # Push the random number to XCom
    kwargs['ti'].xcom_push(key='random_number', value=random_number)
    print(f"Generated random number: {random_number}")

# Function to double the random number
def double_random_number(**kwargs):
    # Pull the random number from XCom
    ti = kwargs['ti']
    random_number = ti.xcom_pull(task_ids='generate_random_task', key='random_number')
    
    doubled_number = random_number * 2
    # Push the doubled number to XCom
    ti.xcom_push(key='doubled_number', value=doubled_number)
    print(f"Doubled number: {doubled_number}")

# Function to print the final value
def print_final_value(**kwargs):
    # Pull the doubled number from XCom
    ti = kwargs['ti']
    doubled_number = ti.xcom_pull(task_ids='double_random_task', key='doubled_number')
    
    print(f"Final value received: {doubled_number}")

with DAG(
    dag_id='xcom_example',
    default_args=default_args,
    schedule_interval='@once',  # Run once for demonstration
    catchup=False,
) as dag:

    start, end = create_common_tasks(dag)
    
    generate_random_task = PythonOperator(
        task_id='generate_random_task',
        python_callable=generate_random_number,
        provide_context=True,
    )

    double_random_task = PythonOperator(
        task_id='double_random_task',
        python_callable=double_random_number,
        provide_context=True,
    )

    print_final_task = PythonOperator(
        task_id='print_final_task',
        python_callable=print_final_value,
        provide_context=True,
    )
       
    start >> generate_random_task >> double_random_task >> print_final_task >> end
