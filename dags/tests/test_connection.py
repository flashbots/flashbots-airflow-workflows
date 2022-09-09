import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "mev_db_test_dag"

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(2022,9,6),
    schedule_interval = "@once",
    catchup = False,
) as dag:
    get_count = PostgresOperator(
        postgres_conn_id="mev_db",
        task_id = "get_count",
        sql = "select count(*) from measured_block_profitability;"
    )

    get_one = PostgresOperator(
        postgres_conn_id="mev_db",
        task_id = "get_one",
        sql = "select count(*) from measured_block_profitability limit 1;"
    )

    get_count >> get_one
