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
        sql=f"""with past_24h_submissions as (SELECT builder_pubkey, count(builder_pubkey) as blocks FROM mainnet_payload_delivered WHERE inserted_at BETWEEN NOW() - INTERVAL '1 DAYS' AND NOW() GROUP BY builder_pubkey),
past_24h_top_pubkeys as (select a.builder_pubkey, a.blocks, b.description FROM past_24h_submissions a LEFT JOIN mainnet_blockbuilder b ON a.builder_pubkey = b.builder_pubkey where a.blocks > 10 ORDER BY blocks DESC LIMIT 20),
total_blocks as (select count(*) as cnt from mainnet_payload_delivered WHERE inserted_at BETWEEN NOW() - INTERVAL '1 DAYS' AND NOW())
SELECT builder_pubkey, blocks::int, description FROM past_24h_top_pubkeys
union all
select 'others' as builder_pubkey, (select cnt from total_blocks) - (select sum(blocks) from past_24h_top_pubkeys) as blocks, 'others' as description""",
        slack_conn_id=SLACK_CONN_ID,
        slack_message="Builders submitting to the relay, past 24 hours\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys')}}```",
    )

    SqlToSlackOperator(
        task_id="daily_orgs_stats_to_slack",
        sql_conn_id=POSTGRES_CONN_ID,
        sql=f"""with desc_map (k, v) as (values ('fb-merger1', 'flashbots'), ('fb-merger2', 'flashbots'), ('fb-merger3', 'flashbots'), ('bloXroute max-profit', 'bloXroute')), 
total_blocks as (select count(*) as cnt from mainnet_payload_delivered WHERE inserted_at BETWEEN NOW() - INTERVAL '1 DAYS' AND NOW()),
past_24h_submissions as (SELECT builder_pubkey, count(builder_pubkey) as blocks FROM mainnet_payload_delivered WHERE inserted_at BETWEEN NOW() - INTERVAL '1 DAYS' AND NOW() GROUP BY builder_pubkey),
formatted_orgs as (select b.builder_pubkey, coalesce(dm.v, nullif(b.description, ''), left(b.builder_pubkey, 8) || '..' || right(b.builder_pubkey, 6)) as org from mainnet_blockbuilder b left join desc_map dm on dm.k = b.description group by b.builder_pubkey, dm.v, b.description)
SELECT fo.org as org, sum(a.blocks)::int as blocks, TO_CHAR(100*sum(a.blocks)/(select cnt from total_blocks), 'fm90D0%') as share FROM past_24h_submissions a
    LEFT JOIN formatted_orgs fo on fo.builder_pubkey = a.builder_pubkey
group by fo.org ORDER BY blocks DESC LIMIT 10""",
        slack_conn_id=SLACK_CONN_ID,
        slack_message="Builder orgs submitting to the relay, past 24h\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys')}}```",
    )
