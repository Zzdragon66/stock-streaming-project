import pytz
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from datetime import datetime, timedelta
from time import sleep
from concurrent.futures import ProcessPoolExecutor

import sys
sys.path.append("/opt/airflow/scripts/stock_data_generation/")
from single_stock_generator import SingleStockGenerator

DATA_URL = "https://api.polygon.io/v2/aggs/ticker/{stock}/range/1/second/{start_time}/{end_time}?adjusted=true&sort=asc&limit=1400&apiKey={api_key}"
EAST_TIMEZONE = pytz.timezone("US/Eastern")
API_LIMIT = 300 # the number of calls / miniute
DEPLAY_MINNUTE = 16

def date_to_time(date_str : str):
    """Utility function to convert date string to dates"""
    format = "%Y-%m-%d"
    time_val = datetime.strptime(date_str, format)
    return time_val

class StockGenerator():

    def __init__(self, stock : str, kafka_servers : str, topic : str, api_key : str, date_str : str, idx : int):
        """_summary_

        Args:
            stock (str): the stock symbol
            kafka_servers (str): kafka producer servers
            topic (str): the topic name in kafka server
            api_key (str): api key in the stock 
            date_str (str): the date string
            idx (int): partition index
            n_nodes (int, optional): _description_. Defaults to 1.
        """
        self.stock = stock
        self.kafka_servers = kafka_servers
        self.today = date_to_time(date_str) 
        # initialize the kafka topic and kafka servers 
        conf = {
            "bootstrap.servers" : kafka_servers
        }
        self.generator = SingleStockGenerator(api_key, DATA_URL, Producer(conf), stock, topic, idx) 
    
    def trading_hours(self, date):
        """
            Generate the upper bound and lower bound of the trading hour UTC time in millisecond
        Returns: 
            utc_lower_time : the lower utc time of the trading hour NOT TIMESTAMP
            utc_upper_time : the upper utc time of the trading hour NOT TIMESTAMP
        """
        lower_time = date.replace(hour = 9, minute=30, second = 0, microsecond = 0, tzinfo=EAST_TIMEZONE) # EASTERN TIME LOWER BOUND
        upper_time = date.replace(hour = 16, minute=00, second = 0, microsecond = 0, tzinfo=EAST_TIMEZONE) # EASTERN TIME UPPER BOUND
        utc_lower_time = lower_time.astimezone(pytz.UTC)
        utc_upper_time = upper_time.astimezone(pytz.UTC)
        return utc_lower_time, utc_upper_time

    def realtime_stock_generation(self):
        # Get weekday data
        today = self.today
        if today.weekday() > 4:
            print("Today is not trading day")
            return
        # Generate the UTC trading time range
        utc_lower_time, utc_upper_time = self.trading_hours(today)
        trading_time = utc_lower_time

        while trading_time <= utc_upper_time:
            # if trading time is greater than cur_time(delayed 16mins) -> sleep 
            cur_time = datetime.now().astimezone(pytz.UTC) - timedelta(minutes=DEPLAY_MINNUTE)
            print("cur_time is: ", cur_time, "trading_time is ", trading_time)
            if cur_time < trading_time:
                # trading scraping time is ahead of the current time
                time_off_second = (trading_time - cur_time).seconds
                sleep(time_off_second)
            # get the next time 
            next_time = trading_time + timedelta(seconds=10)
            lower_timestamp, upper_timestamp = int(trading_time.timestamp() * 1000), int(next_time.timestamp() * 1000)
            self.generator.produce_single_stock_data(lower_timestamp, upper_timestamp)
            trading_time = next_time
            sleep(0.5)