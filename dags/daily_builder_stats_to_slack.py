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
    start_date = datetime.datetime(2022, 10, 20),
    schedule_interval = '0 8 * * *', # daily 8 am utc
    catchup = False,
) as dag:

    SqlToSlackOperator(
        task_id="daily_builders_stats_to_slack",
        sql_conn_id=POSTGRES_CONN_ID,
        sql=f"""
SELECT a.builder_pubkey, a.blocks, b.description FROM (
    SELECT builder_pubkey, count(builder_pubkey) as blocks FROM mainnet_payload_delivered
        WHERE inserted_at BETWEEN NOW() - INTERVAL '1 DAYS' AND NOW()
        GROUP BY builder_pubkey) a
    LEFT JOIN  mainnet_blockbuilder b ON a.builder_pubkey = b.builder_pubkey
ORDER BY blocks DESC LIMIT 10""",
        slack_conn_id=SLACK_CONN_ID,
        slack_message="Builder stats for the last 24 hours\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys')}}```",
    )

    SqlToSlackOperator(
        task_id="daily_orgs_stats_to_slack",
        sql_conn_id=POSTGRES_CONN_ID,
        sql=f"""with desc_map (k, v) as (values ('fb-merger1', 'flashbots'), ('fb-merger2', 'flashbots'), ('fb-merger3', 'flashbots'), ('bloXroute max-profit', 'bloXroute'))
SELECT coalesce(dm.v, nullif(b.description, ''), b.builder_pubkey) as org, sum(a.blocks) as blocks FROM ( SELECT builder_pubkey, count(builder_pubkey) as blocks
FROM mainnet_payload_delivered WHERE inserted_at BETWEEN NOW() - INTERVAL '1 DAYS' AND NOW() GROUP BY builder_pubkey) a
    LEFT JOIN  mainnet_blockbuilder b ON a.builder_pubkey = b.builder_pubkey
    left join desc_map dm on dm.k = b.description
group by coalesce(dm.v, nullif(b.description, ''), b.builder_pubkey) ORDER BY blocks DESC LIMIT 10""",
        slack_conn_id=SLACK_CONN_ID,
        slack_message="Orgs stats for the last 24 hours\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys')}}```",
    )
