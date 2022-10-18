import datetime
import boto3
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
DAG_ID = "relay_export_dag"
POSTGRES_CONN_ID = "boost_relay_read_replica"
AWS_REGION = "us-east-2"

session = boto3.session.Session()
client = session.client(service_name='secretsmanager',region_name=AWS_REGION)
secret = json.loads(client.get_secret_value(SecretId=SECRET)["SecretString"])

BUCKET_NAME = secret["bucket_name"]

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(2022,10,17),
    schedule_interval = "@once",
    catchup = True,
) as dag:

    def _fetch_and_export_relay_data():

        psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = psql_hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT *
                FROM mainnet_builder_block_submission
                WHERE inserted_at > '{datetime.datetime(2022,10,17)}'
                LIMIT 1
            """)
            result = cursor.fetchall()


        s3_hook = S3Hook('s3_conn')
        s3_hook.load_file(filename=f'{datetime.datetime.now()}', key=f'{datetime.datetime.now()}', bucket_name=BUCKET_NAME)

    fetch_and_export_relay_data = PythonOperator(
    task_id="fetch_and_export_relay_data",
    python_callable=_fetch_and_export_relay_data
    )
    fetch_and_export_relay_data
