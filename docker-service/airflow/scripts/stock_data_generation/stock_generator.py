import pytz
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from datetime import datetime, timedelta
from time import sleep
from concurrent.futures import ThreadPoolExecutor

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

    def __init__(self, stocks : list[str], kafka_servers : str, topic : str, api_key : str, date_str : str, n_nodes = 1):
        self.stocks = stocks 
        self.kafka_servers = kafka_servers
        self.today = date_to_time(date_str) 
        # initialize the kafka topic and kafka servers 
        conf = {
            "bootstrap.servers" : kafka_servers
        }
        admin_client = AdminClient(conf)
        if topic not in admin_client.list_topics().topics:
            topic_lst = [NewTopic(topic, num_partitions=len(stocks), replication_factor=n_nodes)]
            fs = admin_client.create_topics(topic_lst)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic {topic} created")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else:
            print(f"Topic {topic} is already created")
        # creat the produce
        self.producer = Producer(conf)
        # Initialize the stock generation instance
        self.stock_generators = []
        for idx, stock_name in enumerate(stocks):
            self.stock_generators.append(SingleStockGenerator(api_key, DATA_URL, self.producer, stock_name, topic, idx))
    
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
            with ThreadPoolExecutor(max_workers=len(self.stocks)) as executor:
                for idx, _ in enumerate(self.stocks):
                    executor.submit(self.stock_generators[idx].produce_single_stock_data, lower_timestamp, upper_timestamp) 
                print(f"Finish time range {trading_time}, {next_time} data generation")
            trading_time = next_time
            sleep(0.5)

    def history_stock_generation(self, start_date : str, end_date : str, scrape_interval = 1200):
        """
            Generate the historical stock data from the start_date to end_date
        Args:
            start_date : a string representing the start date with format yyyy-mm-dd
            end_date : a string representing the end date with the format yyyy-mm-dd
            scrape_interval : 
        """
        start_date, end_date = date_to_time(start_date), date_to_time(end_date)
        cur_date = start_date
        while cur_date <= end_date:
            cur_week_idx = cur_date.weekday() 
            if cur_week_idx > 4:
                cur_date += timedelta(days = 7 - cur_week_idx)
                continue
            utc_lower_time, utc_upper_time = self.trading_hours(cur_date)
            cur_time = utc_lower_time
            while cur_time <= utc_upper_time: 
                nxt_time = cur_time + timedelta(seconds=scrape_interval)
                lower_timestamp = int(cur_time.timestamp() * 1000)
                upper_timestamp = int(nxt_time.timestamp() * 1000)
                with ThreadPoolExecutor(max_workers=len(self.stocks)) as executor:
                    for idx, _ in enumerate(self.stocks):
                        executor.submit(self.stock_generators[idx].produce_single_stock_data, lower_timestamp, upper_timestamp) 
                    print(f"Finish time range {cur_time}, {nxt_time} data generation")
                # update the time 
                cur_time = nxt_time
                sleep(1)
            # update the date
            cur_date += timedelta(days=1)
