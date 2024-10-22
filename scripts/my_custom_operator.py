from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
import psycopg2

class DataQualityCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self, table, column, postgres_conn_id, *args, **kwargs):
        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.column = column
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        conn = self.get_connection(self.postgres_conn_id)
        with conn.cursor() as cursor:
            # Check for null values
            cursor.execute(f"SELECT COUNT(*) FROM {self.table} WHERE {self.column} IS NULL;")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                raise ValueError(f"Data quality check failed. Found {null_count} null values in {self.column} of {self.table}.")

            # Check for duplicates
            cursor.execute(f"SELECT COUNT(DISTINCT {self.column}), COUNT(*) FROM {self.table};")
            count_distinct, count_total = cursor.fetchone()
            if count_distinct < count_total:
                raise ValueError(f"Data quality check failed. Found duplicates in {self.column} of {self.table}.")
            
            self.log.info("Data quality check passed.")

    def get_connection(self, conn_id):
        # Use BaseHook to retrieve the connection object
        conn = BaseHook.get_connection(conn_id)
        return psycopg2.connect(
            host=conn.host,
            database=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        )
