import pandas as pd
import numpy as np
import torch
from torch import nn
import joblib
from time import sleep
import pytz
import argparse
from pathlib import Path
from lstm.VanillaLSTM import VanillaLSTM

from database import CassandraDB
from kafka import KafkaConsumer 
from data_preprocess import data_preprocess

STOCK = "SPY"
EAST_TIMEZONE = pytz.timezone("US/Eastern")
STOCK_TABLE = "stock_table"
FEATURE_COLS = ["volume", "curr_price", "high_price", "low_price", "n_transactions"]
TARGET_COL = "curr_price"
DEPLAY_MINNUTE = 15

DEVICE = None
if torch.cuda.is_available():
    DEVICE = torch.device("cuda")
elif torch.backends.mps.is_available():
    DEVICE = torch.device("mps")
else:
    DEVICE = torch.device("cpu")
print(DEVICE)

    
def load_scaler(path : str):
    """Load the scaler at the path"""
    if not Path(path).exists():
        raise FileNotFoundError("Scaler is not found")
    return joblib.load(path)

def scale_data(scaler, input_df):
    return scaler.transform(input_df)

def inverse_scale(scaler, return_val : np.array, arr_idx : int = 1):
    """Inverse transform the data back"""
    min_price = scaler.data_min_[arr_idx]
    range_price = scaler.data_range_[arr_idx]
    return return_val * range_price + min_price

def load_lstm_model(D = 20, model_path="lstm/vanilla_lstm_params.pth"):
    """Load the model weights into model and return the LSTM Model"""
    model = VanillaLSTM(input_dim = D,
            hidden_size = 50, device = DEVICE,
            )
    model = model.to(DEVICE)
    print(model_path)
    model.load_state_dict(torch.load(model_path, map_location=DEVICE))
    model.eval()
    return model


def stock_prediction_main(cassandra_cluster, kafka_cluster, topic_name, partition):
    """main function for stock prediction"""
    db = CassandraDB(cassandra_cluster)
    kafkaConsumer = KafkaConsumer(kafka_cluster, topic_name, partition)
    scaler = load_scaler("./scalar.save")

    

    while True:
        rows = kafkaConsumer.get_data()

        rows_df = pd.DataFrame(rows)
        curr_timestamp, curr_price = rows_df.loc[:, ["utc_timestamp", "curr_price"]].iloc[-1]
        curr_timestamp = int(curr_timestamp)

        input_data = rows_df.loc[:, FEATURE_COLS].iloc[:-1, :]
        input_data = scale_data(scaler, input_data)
        input_data = data_preprocess(input_data.reshape(1, *input_data.shape))

        input_tensor = torch.tensor(input_data, dtype=torch.float32, device=DEVICE)
        # get the model
        N, T, D = input_tensor.shape
        lstm = load_lstm_model(D)
        # predict model
        lstm_predict_price = lstm(input_tensor).cpu().item()
        lstm_predict_price = inverse_scale(scaler, lstm_predict_price)
        # insert the data 
        insert_stmt = f"""INSERT INTO stock.predict_table (stock, utc_timestamp, curr_price, predict_price) VALUES ('{STOCK}', {curr_timestamp}, {curr_price}, {lstm_predict_price});"""
        # write back data
        db.write_databack(insert_stmt)
        # give machine some rest
        sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cassandra_cluster", type=str, required=True)
    parser.add_argument("--kafka_cluster", type=str, required=True)
    parser.add_argument("--topic_name", type=str, required=True)
    parser.add_argument("--partition", type=str, required=True)
    args = parser.parse_args()
    stock_prediction_main(args.cassandra_cluster, args.kafka_cluster, args.topic_name, int(args.partition))


 
if __name__ == "__main__":
    main()