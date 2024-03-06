from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import json
import os

from airflow import DAG
from airflow import settings
from airflow.models import Connection, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Add the script into the path
import sys
import json
sys.path.append("/opt/airflow/scripts/")
from stock_data_generation.stock_generator import StockGenerator

from datetime import datetime, timedelta
import pytz

start_date = datetime(2024, 2, 28, 10, 30, tzinfo=ZoneInfo("America/Los_Angeles"))
KAFKA_PRODUCER_SERVER = Variable.get("KAFKA_PRODUCER_SERVERS")
KAFKA_CONSUMER_SERVER = Variable.get("KAFKA_CONSUMER_SERVERS")
REDDIT_CLIENT = Variable.get("CLIENT_ID")
REDDIT_SECRET = Variable.get("CLIENT_SECRET")
CASS_CLUSTER = Variable.get("CASSANDRA_CLUSTER")
API_KEY = Variable.get("STOCK_API")
today = datetime.today().astimezone(pytz.timezone("US/Eastern")).date()
#today = datetime(2024, 3, 6).astimezone(pytz.timezone("US/Eastern")).date()
TOPIC = f"""{Variable.get("TOPIC")}-{today}"""
STOCKS = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "AMZN", "SPY"]


def generate_realtime_stock_data():
    print(today)
    stock_generator = StockGenerator(
        stocks = STOCKS,
        kafka_servers = KAFKA_PRODUCER_SERVER,
        topic = TOPIC,
        api_key = API_KEY,
        date_str = str(today)
    )
    stock_generator.realtime_stock_generation()

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
    realtime_stock = PythonOperator(
        task_id = "stock_data_generator",
        python_callable=generate_realtime_stock_data
    )
    realtime_reddit = BashOperator(
        task_id = "reddit_generator",
        bash_command=f"""python3 /opt/airflow/scripts/reddit-producer.py \
        --reddit_client {REDDIT_CLIENT} \
        --reddit_secret {REDDIT_SECRET} \
        --cassandra_host {CASS_CLUSTER}"""
    )

    initialize_cassandra_table >> realtime_stock
    initialize_cassandra_table >> realtime_reddit 