import datetime
import boto3
import json
from airflow import DAG
from airflow.providers.slack.transfers.sql_to_slack import SqlToSlackOperator
from airflow.models import Variable

POSTGRES_CONN_ID = Variable.get('boost_relay_postgres_conn_id')
SLACK_CONN_ID = Variable.get('slack_conn_id')

with DAG(
    dag_id = "builders_blocks_stats",
    start_date = datetime.datetime(2022,10,19),
    schedule_interval = "@daily",
    catchup = False,
) as dag:

    SqlToSlackOperator(
        task_id="daily_builders_stats_to_slack",
        sql_conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT builder_pubkey, count(builder_pubkey) as blocks FROM mainnet_payload_delivered WHERE inserted_at BETWEEN NOW() - INTERVAL '24 HOURS' AND NOW() GROUP BY builder_pubkey ORDER BY blocks DESC LIMIT 10",
        slack_conn_id=SLACK_CONN_ID,
        slack_message="Builder stats {{ ds }}\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys')}}```",
    )
