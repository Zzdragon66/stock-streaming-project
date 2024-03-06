from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import argparse

def initialize_table(cassandra_cluster : str, keyspace_name : str, stock_table : str, n_nodes = 1):
    """
        Initialize the Cassandra keyspace and table
    args:
        keyspace_name : the keyspace in the cassandra
        stock_name : the table in the cassandra
        volume_table : the volumne table in the cassandra
    """
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster([cassandra_cluster], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    # Create the keyspace 
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '{n_nodes}' }}
    """)
    session.set_keyspace(keyspace_name)
    # Create the table 
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {stock_table} (
            stock text,
            utc_timestamp timestamp,
            curr_price double,
            high_price double,
            low_price double,
            n_transactions int,
            volume double,
            volume_cumulative double,
            PRIMARY KEY((stock), utc_timestamp)
        ) WITH CLUSTERING ORDER BY (utc_timestamp ASC)
    """)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cassandra_cluster", required=True, type=str)
    parser.add_argument("--keyspace", type=str, required=True)
    parser.add_argument("--stock_table", type=str, required=True)
    parser.add_argument("--n_nodes", type=int, default=1)
    args = parser.parse_args()
    initialize_table(args.cassandra_cluster, args.keyspace, args.stock_table, args.n_nodes) 

if __name__ == "__main__":
    main()