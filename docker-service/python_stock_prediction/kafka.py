from confluent_kafka import Consumer, TopicPartition
import json 


def process_msg(msg):
    msg_value = json.loads(msg.value().decode('utf-8'))
    return msg_value

class KafkaConsumer:
    def __init__(self, kafka_cluster : str, topic_name : str, partition : int) -> None:
        conf = {"bootstrap.servers" : kafka_cluster, "group.id": "deep_learning_model"}
        self.consumer = Consumer(conf)
        tp = TopicPartition(topic_name, partition, offset = 0)
        self.consumer.assign([tp])
        self.rows  = []
    
    def poll(self, timeout: float = 1):
        return self.consumer.poll(timeout)

    def close(self):
        self.consumer.close()

    def get_data(self, lookback: int = 1001):
        """Idea: keep pooling until there is enough data"""
        while True:
            msg = self.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            # there is message
            msg_value = process_msg(msg)
            self.rows.append(msg_value)
            if len(self.rows) < lookback:
                continue
            elif len(self.rows) == lookback:
                return self.rows
            else:
                self.rows = self.rows[1:]
                return self.rows

    
