from confluent_kafka.admin import AdminClient, NewTopic

# Initialize the kafka topic

def create_kafka_topic(kafka_servers : str, topic : str, n_stocks : int, n_nodes = 1):
    """Create the kafka topic for the kafka server"""
    conf = {
            "bootstrap.servers" : kafka_servers
    }
    admin_client = AdminClient(conf)
    if topic not in admin_client.list_topics().topics:
        topic_lst = [NewTopic(topic, num_partitions=n_stocks, replication_factor=n_nodes)]
        fs = admin_client.create_topics(topic_lst)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print(f"Topic {topic} is already created")
