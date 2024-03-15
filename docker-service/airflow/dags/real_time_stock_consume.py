from datetime import datetime, timedelta
import pytz
from zoneinfo import ZoneInfo
from pathlib import Path
import json
import os
from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import sys
import json

start_date = datetime(2024, 3, 15, 10, 30, tzinfo=ZoneInfo("America/Los_Angeles"))
KAFKA_PRODUCER_SERVER = Variable.get("KAFKA_PRODUCER_SERVERS")
KAFKA_CONSUMER_SERVER = Variable.get("KAFKA_CONSUMER_SERVERS")
SPARK_CLUSTER = Variable.get("SPARK_CLUSTER")
CASS_CLUSTER = Variable.get("CASSANDRA_CLUSTER")
API_KEY = Variable.get("STOCK_API")
today = datetime.today().astimezone(pytz.timezone("US/Eastern")).date()
STOCK_PREDICTION_IMAGE = Variable.get("STOCK_PREDICTION_IMAGE")
#today = datetime(2024, 3, 6).replace(tzinfo = pytz.timezone("US/Eastern")).date()
TOPIC = f"""{Variable.get("TOPIC")}-{today}"""
STOCKS = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "AMZN", "SPY"]

today = datetime.today().astimezone(pytz.timezone("US/Eastern")).date()

with DAG(
    dag_id = "Real-Time-Data-Streaming",
    start_date = start_date,
    schedule='@daily',
    tags=["stock"]
) as dag:
    consume_data = BashOperator(
        task_id = "StreamProcessingData",
        bash_command= f"""
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /opt/airflow/scripts/spark/spark_stream_processing.py \
            --spark_cluster {SPARK_CLUSTER} \
            --kafka_cluster {KAFKA_CONSUMER_SERVER} \
            --topic {TOPIC} \
            --cassandra_cluster {CASS_CLUSTER} \
            --keyspace stock \
            --table_name stock_table \
            --cassandra_user cassandra \
            --cassandra_pwd cassandra \
        """
    )
    predict_stock = KubernetesPodOperator(
            namespace='airflow',
            image= STOCK_PREDICTION_IMAGE + ":latest", 
            cmds=['python3', 'stock_prediction_main.py'],
            arguments=[
                '--cassandra_cluster', CASS_CLUSTER,
                '--kafka_cluster', KAFKA_CONSUMER_SERVER,
                '--topic_name', TOPIC,
                '--partition', str(len(STOCKS) - 1)
            ],
            name=f'predict_stock-{today}',
            task_id=f'predict_stock-{today}',
            get_logs=True,
            in_cluster = True,
            is_delete_operator_pod=True,
            image_pull_policy='Always'
        )
    consume_data
    predict_stock
