import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
DAG_ID = "relay_export_dag"
POSTGRES_CONN_ID = "boost_relay_read_replica"

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(2022,10,17),
    schedule_interval = "@hourly",
    catchup = True,
) as dag:

    def _get_relay_data(ti):

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT * 
                FROM mainnet_builder_block_submission 
                WHERE inserted_at > {datetime.datetime(2022,10,17)}
            """)
            result = cursor.fetchall()

        ti.xcom_push(key="block_query_result",value=result)
    
    def _export_to_s3(ti):
        pass

    def _export_to_rds(ti):
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"")

        ti.xcom_pull(key="block_query_result",task_ids="get_relay_data")


    get_relay_data = PythonOperator(
    task_id="get_relay_data",
    python_callable=_get_relay_data
    )
    export_to_s3 = PythonOperator(
        task_id="export_to_s3",
        python_callable=_export_to_s3
    )
    export_to_rds= PythonOperator(
        task_id="export_to_rds",
        python_callable=_export_to_rds
    )


    get_relay_data >> export_to_s3 >> export_to_rds