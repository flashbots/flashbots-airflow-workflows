import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
DAG_ID = "relay_export_dag"
POSTGRES_CONN_ID = "boost_relay_read_replica"

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(2022,10,17),
    schedule_interval = "@once",
    catchup = True,
) as dag:

    def _get_relay_data(ti):

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"")

        ti.xcom_push(key="block",task_ids="get_onchain_data")

        pass

    def _export_to_rds(ti):
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"")

        ti.xcom_pull(key="block",task_ids="get_onchain_data")
        pass

    get_relay_data= PythonOperator(
    task_id="get_balance_change",
    python_callable=_get_relay_data
    )
    export_to_rds= PythonOperator(
        task_id="write_to_database",
        python_callable=_export_to_rds
    )

    get_relay_data >> export_to_rds