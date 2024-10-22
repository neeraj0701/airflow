from airflow.operators.empty import EmptyOperator

# Define a function that creates a set of tasks/operators
def create_common_tasks(dag):
    start_task = EmptyOperator(
        task_id='start',
        dag=dag
    )
    
    end_task = EmptyOperator(
        task_id='end',
        dag=dag
    )
    
    return start_task, end_task