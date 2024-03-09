from datetime import datetime
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import sys
import json
from datetime import datetime, timedelta
import pytz

sys.path.append("/opt/airflow/scripts/")
from kafka_topic_creation import create_kafka_topic

start_date = datetime(2024, 2, 28, 10, 30, tzinfo=ZoneInfo("America/Los_Angeles"))
KAFKA_PRODUCER_SERVER = Variable.get("KAFKA_PRODUCER_SERVERS")
KAFKA_CONSUMER_SERVER = Variable.get("KAFKA_CONSUMER_SERVERS")
REDDIT_CLIENT = Variable.get("CLIENT_ID")
REDDIT_SECRET = Variable.get("CLIENT_SECRET")
CASS_CLUSTER = Variable.get("CASSANDRA_CLUSTER")
API_KEY = Variable.get("STOCK_API")
today = datetime.today().astimezone(pytz.timezone("US/Eastern")).date()
#today = datetime(2024, 3, 6).replace(tzinfo=pytz.timezone("US/Eastern")).date()
TOPIC = f"""{Variable.get("TOPIC")}-{today}"""
STOCKS = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "AMZN", "SPY"]

STOCK_GENERATOR_IMAGE = Variable.get("STOCK_GENERATOR_IMAGE")
REDDIT_NEWS_IMAGE = Variable.get("REDDIT_NEWS_IMAGE")
STOCK_PREDICTION_IMAGE = Variable.get("STOCK_PREDICTION_IMAGE")


with DAG(
    dag_id = "Real-Time-Data-Generation",
    start_date = start_date,
    tags=["stock"]
) as dag:
    
    initialize_cassandra_table = BashOperator(
        task_id = "cassandra_init",
        bash_command=f"""python3 /opt/airflow/scripts/cassandra_table_creation.py \
            --cassandra_cluster {CASS_CLUSTER} \
            --keyspace stock \
            --stock_table stock_table \
            """
    )

    kafka_topic_init = PythonOperator(
        task_id = "kafka_init",
        python_callable=create_kafka_topic,
        op_kwargs = {
            "kafka_servers" : KAFKA_PRODUCER_SERVER,
            "topic" : TOPIC,
            "n_stocks" : len(STOCKS)
        } 
    )

    # Dynamic DAG generation 
    stock_generators = []
    for idx, stock in enumerate(STOCKS):
        realtime_stock = KubernetesPodOperator(
            namespace='airflow',
            image= STOCK_GENERATOR_IMAGE + ":latest", 
            cmds=['python3', 'stock_generation_script.py'],
            arguments=[
                '--stock', stock,
                '--kafka_server', KAFKA_PRODUCER_SERVER,
                '--topic', TOPIC,
                '--api_key', API_KEY,
                '--date_str', str(today),
                "--idx", str(idx)
            ],
            name=f'stock-generator-{today}-{idx}',
            task_id=f'stock_data_generator-{today}-{idx}',
            get_logs=True,
            in_cluster = True,
            is_delete_operator_pod=True,
            image_pull_policy='Always'
        )
        stock_generators.append(realtime_stock)

    realtime_reddit = KubernetesPodOperator(
        namespace='airflow',
        image= REDDIT_NEWS_IMAGE + ":latest",
        
        cmds=['python3', 'reddit-producer.py'],
        arguments=[
            '--reddit_client', REDDIT_CLIENT,
            '--reddit_secret', REDDIT_SECRET,
            '--cassandra_host', CASS_CLUSTER,
        ],
        name=f'reddit-news-{today}',
        task_id=f'reddit_news-{today}',
        get_logs=True,
        in_cluster = True,
        is_delete_operator_pod=True,
        image_pull_policy='Always'
    )

    initialize_cassandra_table >> stock_generators
    kafka_topic_init >> stock_generators

    initialize_cassandra_table >> realtime_reddit 