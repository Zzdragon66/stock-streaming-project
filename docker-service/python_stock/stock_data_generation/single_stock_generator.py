import requests
import json
DICT_MAPPING = {"v":"volume", 'c' : "curr_price", "h" : "high_price", "l" : "low_price", "t" : "utc_timestamp", "n" : "n_transactions"}
class SingleStockGenerator():
    """stock data generator"""
    def __init__(self, api_key : str, template_url : str, producer, stock : str, topic_name : str, part_idx : int):
        self.templated_url = template_url
        self.producer = producer
        self.stock = stock
        self.topic_name = topic_name
        self.part_idx = part_idx
        self.api_key = api_key
        self.cumsum = 0
    
    def acked(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            print(f"Failed to deliver message: {err}")
        else:
            print(f"Message produced: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def produce_single_stock_data(self, start_time, end_time):
        """Proudce the stock data and produce the data to the kafka cluster
        Args:
            start_time (int): UTC time stamp in millisecond format
            end_time (int): UTC time stamp in millisecond format
        Raises:
            requests.exceptions.RequestException: _description_
        """
        URL = self.templated_url.format(
            stock = self.stock,
            start_time = start_time,
            end_time = end_time,
            api_key = self.api_key
        )
        response = requests.get(URL, timeout=10)
        # RETRY 3 times if the status code is not 200
        retry_cnt = 0
        while response.status_code != 200:
            retry_cnt += 1
            if retry_cnt == 3:
                raise requests.exceptions.RequestException("URL did not work")
        if (json.loads(response.text)["resultsCount"]) == 0:
            return
        results = (json.loads(response.text)["results"])
        sorted_results = sorted(results, key=lambda x: x['t'])
        for sorted_result in sorted_results:
            key = json.dumps({"stock" : self.stock}).encode("utf-8")
            values = {DICT_MAPPING[key] : sorted_result[key] for key in DICT_MAPPING}
            # update the cum_sum and add the value to the values 
            self.cumsum += int(values["volume"])
            values["volume_cumulative"] = self.cumsum
            values = json.dumps(values).encode("utf-8")
            # produce the result
            self.producer.produce(self.topic_name, key=key, value=values, partition=self.part_idx, callback=self.acked)
        self.producer.flush()

