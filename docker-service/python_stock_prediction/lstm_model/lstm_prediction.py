import pandas as pd
import numpy as np
import torch
from torch import nn
import joblib
from time import sleep
from datetime import datetime, timedelta
import pytz
import argparse
import json
from confluent_kafka import Consumer, TopicPartition
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
from lstm_model import LSTMModel


EAST_TIMEZONE = pytz.timezone("US/Eastern")
STOCK_TABLE = "stock_table"
FEATURE_COLS = ["curr_price", "volume", "n_transactions", "high_price", "low_price"]
DEPLAY_MINNUTE = 15

DEVICE = None
if torch.cuda.is_available():
    DEVICE = torch.device("cuda")
elif torch.backends.mps.is_available():
    DEVICE = torch.device("mps")
else:
    DEVICE = torch.device("cpu")
print(DEVICE)


def load_lstm_model(model_path="lstm_model_weights.pth"):
    """Load the model weights into model and return the LSTM Model
    Args:
        model_path (str, optional): _description_. Defaults to "lstm_model_weights.pth".

    Returns:
        model
    """
    model = LSTMModel(input_dim=5, hidden_dim=200, num_layers=5, output_dim=1)
    model = model.to(DEVICE)
    print(model_path)
    model.load_state_dict(torch.load(model_path, map_location=DEVICE))
    model.eval()
    return model

def date_to_time(date_str : str):
    """Utility function to convert date string to dates"""
    format = "%Y-%m-%d"
    time_val = datetime.strptime(date_str, format)
    return time_val


def init_db_table(cassandra_cluster: str, keyspace_name: str):
    """Initialize the database table in the cassandra cluster"""
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
    cluster = Cluster([cassandra_cluster], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(keyspace_name)
    db_tablestr = """
        CREATE TABLE IF NOT EXISTS predict_table(
            stock text,
            utc_timestamp timestamp,
            curr_price double,
            predict_price double,
            PRIMARY KEY((stock), utc_timestamp)
        ) WITH CLUSTERING ORDER BY (utc_timestamp ASC);
    """
    session.execute(db_tablestr)
    print("Finish initializing the table")
    return session


def write_databack(session, keyspace_name : str, db_str : str):
    """Writet the database with the db_str"""
    try:
        session.execute(db_str)
    except:
        raise Exception("Write back failed")

def prcess_fst_batch(messages):
    rows = []
    for msg in messages:
        msg_value = json.loads(msg.value().decode('utf-8')) 
        rows.append(msg_value)
    return rows

def incremental_changes(old_rows, new_message):
    "Delete the oldest data and append newest data"
    msg_value = json.loads(new_message.value().decode('utf-8')) 
    old_rows.append(msg_value)
    return old_rows[1:]


def generate_prediction(consumer, cass_cluster):
    """
        Generate the predict data to the cassandra table
    Input:
        rows: 
    """
    scalar = joblib.load('scaler.save')
    # load the model
    model = load_lstm_model()
    messages = []
    rows = []
    if_fst_200 = True
    while True:
        msg = consumer.poll(1.0)  # Poll for a message
        if msg is None:
            continue

        # Error Case
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        # Process the first 201 data
        if if_fst_200:
            messages.append(msg)
            if len(messages) >= 201:
                rows = (prcess_fst_batch(messages))
                if_fst_200 = False
        else:
            rows = incremental_changes(rows, msg)
            
        if len(rows) >= 201:
            # Start processing the data
            data = pd.DataFrame(rows).sort_values("utc_timestamp")
            input_data = data[0:-1].loc[:, FEATURE_COLS]
            stock = "SPY"
            utc_timestamp, curr_price = data.loc[:, ["utc_timestamp", "curr_price"]].iloc[-1]
            utc_timestamp = int(utc_timestamp)
            df = pd.DataFrame(scalar.transform(input_data), columns=FEATURE_COLS)
            # load it into torch
            input_tensor = torch.tensor(df.values, dtype=torch.float32, device=DEVICE)
            input_tensor = input_tensor.unsqueeze(0)
            predicted_price = model(input_tensor).item()
            inv_values = np.zeros(len(FEATURE_COLS))
            inv_values[0] = predicted_price
            predicted_stock_price = (scalar.inverse_transform([inv_values])[0][0])
            insert_stmt = f"""INSERT INTO stock.predict_table (stock, utc_timestamp, curr_price, predict_price) VALUES ('{stock}', {utc_timestamp}, {curr_price}, {predicted_stock_price});"""
            write_databack(cass_cluster, "stock", insert_stmt)
            sleep(0.4) # Give some rest to the machine
        

def init_kafka_consumer(kafka_cluster : str, topic_name : str, partition : int):
    conf = {"bootstrap.servers" : kafka_cluster, "group.id": "deep_learning_model"}
    consumer = Consumer(conf)
    tp = TopicPartition(topic_name, partition, offset = 0)
    consumer.assign([tp])
    return consumer

def stock_prediction(cassandra_cluster : str, kafka_cluster : str, topic_name : str, partition : int):
    cass_session = init_db_table(cassandra_cluster, 'stock') # initialize the stock session
    kafka_consumer = init_kafka_consumer(kafka_cluster, topic_name, partition) # initialize the kafka_consumer 
    generate_prediction(kafka_consumer, cass_session) 


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cassandra_cluster", type=str, required=True)
    parser.add_argument("--kafka_cluster", type=str, required=True)
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--partition", type=str, required=True)
    args = parser.parse_args()
    stock_prediction(args.cassandra_cluster, args.kafka_cluster, args.topic_name, int(args.partition))
 
if __name__ == "__main__":
    main()