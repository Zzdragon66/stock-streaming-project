import argparse
import sys
sys.path.append("/opt/airflow/scripts/stock_data_generation/")
from stock_generator import StockGenerator

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock", type=str, required=True)
    parser.add_argument("--kafka_server", type=str, required=True)
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--api_key", type=str, required=True)
    parser.add_argument("--date_str", type=str, required=True)
    parser.add_argument("--idx", type=str, required=True)
    args = parser.parse_args()
    generator = StockGenerator(
        stock = args.stock,
        kafka_servers=args.kafka_server,
        topic = args.topic,
        api_key = args.api_key,
        date_str=args.date_str,
        idx = int(args.idx)
    )
    generator.realtime_stock_generation()

if __name__ == "__main__":
    main()
