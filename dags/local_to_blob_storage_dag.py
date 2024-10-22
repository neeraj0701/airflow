from airflow import DAG
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

def upload_file_to_adls():
    try:
        hook = AzureDataLakeStorageV2Hook(adls_conn_id='adlsgen2')
        hook.upload_file(
            file_system_name=Variable.get("FileSystemName"),
            file_name='AFPractise/Customers.csv',
            file_path=r'/opt/airflow/files/Customers.csv',
            overwrite=True
        )
        
    except Exception as e:
        # Catch any other exceptions
        print(f"An error occurred: {e}")

with DAG(
    'load_data_to_adls_gen2',
    default_args={'owner': 'neeraj', 'start_date': days_ago(1)},
    schedule_interval=None,
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    upload_task = PythonOperator(
    task_id='upload_file_task',
    python_callable=upload_file_to_adls,
    dag=dag
    ) 
    
    end = EmptyOperator(
        task_id='end'
    )

    start >> upload_task >> end
    
