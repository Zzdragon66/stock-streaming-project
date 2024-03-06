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
import sys
import json

# Add the script into the path
sys.path.append("/opt/airflow/scripts/")
from stock_deep_learning.lstm_prediction import stock_prediction


start_date = datetime(2024, 2, 28, 10, 30, tzinfo=ZoneInfo("America/Los_Angeles"))
KAFKA_PRODUCER_SERVER = Variable.get("KAFKA_PRODUCER_SERVERS")
KAFKA_CONSUMER_SERVER = Variable.get("KAFKA_CONSUMER_SERVERS")
SPARK_CLUSTER = Variable.get("SPARK_CLUSTER")
CASS_CLUSTER = Variable.get("CASSANDRA_CLUSTER")
API_KEY = Variable.get("STOCK_API")
today = datetime.today().astimezone(pytz.timezone("US/Eastern")).date()
#today = datetime(2024, 3, 6).astimezone(pytz.timezone("US/Eastern")).date() 
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
    predict_stock = PythonOperator(
        task_id = "PredictStock",
        python_callable = stock_prediction,
        op_kwargs = {
            "cassandra_cluster" : CASS_CLUSTER,
            "kafka_cluster" : KAFKA_CONSUMER_SERVER,
            "topic_name" : TOPIC,
            "partition" : len(STOCKS) - 1
        } 
    )

    consume_data
    predict_stock
