import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
DAG_ID = "goerli_dashboard_dag"
SECRET = "mwaa-airflow-main"
AWS_REGION = "us-east-2"

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(2022,9,13),
    schedule_interval = "@once",
    catchup = False,
) as dag:

    def _get_relay_connection_str(ti)->str:
        import boto3
        import json

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=AWS_REGION
        )

        secret = json.loads(client.get_secret_value(SecretId=SECRET)["SecretString"])

        connection_str: str = json.dumps({}
        )
        ti.xcom_push(key="rpc", value=secret["goerli_rpc"])
        ti.xcom_push(key="connection_str", value=connection_str)


    def _get_latest_proposed_block_hash(ti):
        connection_str = ti.xcom_pull(key="connection_str", task_ids="get_relay_connection_str")

        hook = PostgresHook(connection=connection_str)
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute("")
            result = cursor.fetchone()

        proposer_pubkey = result[0]
        latest_block_hash = result[1]

        ti.xcom_push(key="proposer_pubkey", value=proposer_pubkey)
        ti.xcom_push(key="block_hash", value=latest_block_hash)


    def _get_onchain_data(ti):
        import web3

        rpc = ti.xcom_pull(key="rpc",task_ids="get_relay_connection_str")
        provider = web3.Web3.HTTPProvider(secret["goerli_rpc"])
        w3 = web3.Web3(provider)

        block_hash = ti.xcom_pull(key="block_hash", task_ids="get_latest_proposed_block_hash")
        block = w3.eth.get_block(block_hash)

        ti.xcom_push(key="block", value=block)

    def _get_balance_change(ti):
        from decimal import Decimal
        import web3

        rpc = ti.xcom_pull(key="rpc",task_ids="get_relay_connection_str")
        provider = web3.Web3.HTTPProvider(secret["goerli_rpc"])
        w3 = web3.Web3(provider)

        block_hash = ti.xcom_pull(key="block_hash",task_ids="get_latest_proposed_block_hash")
        proposer_pubkey = ti.xcom_pull(key="proposer_pubkey", task_ids="get_latest_proposed_block_hash")

        validator_balance_before: int = w3.eth.get_balance(proposer_pubkey, block - 1)
        validator_balance_after: int = w3.eth.get_balance(proposer_pubkey, block)
        balance_change: int = validator_balance_after - validator_balance_before
        balance_change_eth: Decimal = web3.fromWei(balance_change)
        ti.xcom_push(key="profit", value=balance_change_eth)


    def _write_to_database(ti):
        block = ti.xcom_pull(key="block",task_ids="get_onchain_data")
        proposer_pubkey = ti.xcom_pull(key="proposer_pubkey", task_ids="get_latest_proposed_block_hash")
        balance_change = ti.xcom_pull(key="profit", task_ids="get_balance_change")

        hook = PostgresHook(postgres_conn_id="")
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            cursor.execute(f"")

    get_relay_connection_str = PythonOperator(
        task_id="get_relay_connection_str",
        python_callable=_get_relay_connection_str
    )
    get_latest_proposed_block_hash = PythonOperator(
        task_id="_get_latest_proposed_block_hash",
        python_callable=_get_latest_proposed_block_hash
    )
    get_onchain_data = PythonOperator(
        task_id="_get_onchain_data",
        python_callable=_get_onchain_data
    )
    get_balance_change = PythonOperator(
        task_id="get_balance_change",
        python_callable=_get_balance_change
    )
    write_to_database = PythonOperator(
        task_id="write_to_database",
        python_callable=_write_to_database
    )

    get_relay_connection_str >> get_latest_proposed_block_hash >> get_onchain_data >> get_balance_change >> write_to_database
