from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import json
import io
import base64

gcs_hook = GCSHook(gcp_conn_id='gcp_conn')
http_hook = HttpHook(http_conn_id="github_api", method="GET")
postgres_hook = PostgresHook(postgres_conn_id='postgresqldb')

bucket_name = Variable.get("bucket_name")
csv_file_path = Variable.get("csv_file_path")
api_endpoints = Variable.get("api_endpoints")
authorization = Variable.get("authorization")
object_path = Variable.get("object_path")
schema = Variable.get("schema")
last_updated_date = json.loads(Variable.get("last_updated_date"))
current_datetime = datetime.now().strftime("%Y-%m-%d")


def update_date_variable(ti):
    value1 = ti.xcom_pull(key='csv_files', task_ids='load_files')
    value2 = ti.xcom_pull(key='tables', task_ids='load_tables')
    value3 = ti.xcom_pull(key='api_endpoints', task_ids='load_api_data')
    value1.update(value2)
    value1.update(value3)
    print(value1,type(value1))
    Variable.set("last_updated_date",json.dumps(value1))

    
def load_files_to_GCS(ti):  
    latest_updatedate = {}
    for file_name in os.listdir(csv_file_path):
        print(file_name)
        if file_name.endswith(".csv"):
            local_file_path = os.path.join(csv_file_path, file_name)
            file = file_name.replace('.csv','')
            print(file)
            df = pd.read_csv(local_file_path)
            df['UpdatedDate'] = pd.to_datetime(df['UpdatedDate'])
            df = df.loc[df['UpdatedDate'] >= last_updated_date[file]]
            get_last_updated_date = df['UpdatedDate'].max()
            last_updated_date[file] = get_last_updated_date.strftime("%Y-%m-%dT%H:%M:%S")
            print(last_updated_date[file])
            print(json.dumps(last_updated_date))
            latest_updatedate[file] = last_updated_date[file]
            print(latest_updatedate)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            print(file)
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=f"{object_path}/{current_datetime}/{file}.csv",
                data=csv_buffer.getvalue()
            )
    ti.xcom_push(key='csv_files', value=latest_updatedate)
            
def load_tables_to_GCS(ti): 
    latest_updatedate = {}
      
    query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE';
    """
    tables = postgres_hook.get_records(query)
    tables = [table[0] for table in tables] 
    
    for table in tables:
        print(table)
        table_data_query = f"SELECT * FROM {schema}.{table} WHERE updateddate >= '{last_updated_date[table].replace('T',' ')}'"
        max_datetime_query = f"SELECT MAX(updateddate) AS latest_datetime FROM {schema}.{table}"
        get_last_updated_date = postgres_hook.get_first(sql=max_datetime_query)[0]
        print(get_last_updated_date)
        last_updated_date[table] = get_last_updated_date.strftime("%Y-%m-%dT%H:%M:%S")
        print(last_updated_date[table])
        print(json.dumps(last_updated_date))
        latest_updatedate[table] = last_updated_date[table]
        print(latest_updatedate)
        table_data = postgres_hook.get_pandas_df(table_data_query)
        print(table_data)
        csv_buffer = io.StringIO()
        table_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=f"{object_path}/{current_datetime}/{table}.csv",
                data=csv_buffer.getvalue()
            )
    ti.xcom_push(key='tables', value=latest_updatedate)
    
            
def load_api_data_to_GCS(ti):    
    latest_updatedate = {}
    
    file_name = api_endpoints.split("/")[-1].split(".")[0]
    print(file_name)
    print(authorization)
    headers= {"Authorization": authorization}
    response = http_hook.run(endpoint=api_endpoints,headers=headers)
    data = response.json()["content"]
    print(data)
    decoded_content = base64.b64decode(data).decode('utf-8')
    json_data = json.loads(decoded_content)
    df = pd.DataFrame(json_data)
    df['UpdatedDate'] = pd.to_datetime(df['UpdatedDate'])
    df = df.loc[df['UpdatedDate'] >= last_updated_date[file_name]]
    get_last_updated_date = df['UpdatedDate'].max()
    last_updated_date[file_name] = get_last_updated_date.strftime("%Y-%m-%dT%H:%M:%S")
    print(last_updated_date[file_name])
    print(json.dumps(last_updated_date))
    latest_updatedate[file_name] = last_updated_date[file_name]
    print(latest_updatedate)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    print(file_name)
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=f"{object_path}/{current_datetime}/{file_name}.csv",
        data=csv_buffer.getvalue()
    )
    ti.xcom_push(key='api_endpoints', value=latest_updatedate)

default_args = {
    'start_date': datetime.today(),
    'retries': 2
}

with DAG(
    'load_to_gcs_bronze',
    default_args=default_args,
    schedule='0 12 * * *',
    catchup=False,
    description='Load data from multiple sources to GCS'
) as dag:

    load_csv_files = PythonOperator(
                task_id="load_files",
                python_callable=load_files_to_GCS,
            )
    
    load_tables = PythonOperator(
                task_id="load_tables",
                python_callable=load_tables_to_GCS,
            )
    
    load_apis = PythonOperator(
                task_id="load_api_data",
                python_callable=load_api_data_to_GCS,
            )
    
    update_date = PythonOperator(
                task_id="update_date",
                python_callable=update_date_variable,
            )
    
    [load_csv_files,load_tables,load_apis] >> update_date