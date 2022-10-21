from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
DAG_ID = "relay_export_dag"
POSTGRES_CONN_ID = "boost_relay_read_replica"

with DAG(
    dag_id = DAG_ID,
    start_date = datetime(2022,10,17),
    schedule_interval = "@daily",
    catchup = True,
) as dag:
    def _fetch_and_export_relay_data():
        import io
        import csv
        BUCKET_NAME = Variable.get('relay-data-export-bucket-name')
        psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = psql_hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT *
                FROM mainnet_builder_block_submission
                WHERE inserted_at > '{datetime.now()}'
                AND inserted_at < '{datetime.now() - timedelta(hours=1)}'
            """)
            result = cursor.fetchall()

        s3_hook = S3Hook('s3_conn')
        s3_hook.load_file_obj(file_obj=io.BytesIO(f"{result}".encode('UTF-8')), key=f'{datetime.now()}', bucket_name=BUCKET_NAME)

    fetch_and_export_relay_data = PythonOperator(
        task_id="fetch_and_export_relay_data",
        python_callable=_fetch_and_export_relay_data
    )

    fetch_and_export_relay_data
