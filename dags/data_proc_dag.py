from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the cluster configuration
CLUSTER_CONFIG = {
        "gce_cluster_config": {
            "zone_uri": "us-central1-a",
            "internal_ip_only": False  # Set this to False
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 100
            }
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 100
            }
        },
}

# Define the job configuration
JOB_CONFIG = {
    "reference": {"project_id": "sapient-origin-438909-p9"},
    "placement": {"cluster_name": "demo-cluster"},
    "spark_job": {
        "main_class": "org.apache.spark.examples.SparkPi",
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "args": ["1000"],
    }
}

# Define the DAG
with DAG(
    'dataproc_cluster_job_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id="sapient-origin-438909-p9",
        cluster_name="demo-cluster",
        cluster_config=CLUSTER_CONFIG,
        gcp_conn_id='data_proc_conn',
        region="us-central1"
    )

    # Task to submit Dataproc job
    submit_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=JOB_CONFIG,
        region="us-central1",
        gcp_conn_id='data_proc_conn',
        project_id="sapient-origin-438909-p9"
    )

    # Task to delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="sapient-origin-438909-p9",
        cluster_name="demo-cluster",
        region="us-central1",
        gcp_conn_id='data_proc_conn'
    )

    # Define task dependencies
    create_cluster >> submit_job >> delete_cluster
