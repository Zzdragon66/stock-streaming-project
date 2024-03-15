# this file is used to perform database operation
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

DB_TABLESTR = """
    CREATE TABLE IF NOT EXISTS predict_table(
        stock text,
        utc_timestamp timestamp,
        curr_price double,
        predict_price double,
        PRIMARY KEY((stock), utc_timestamp)
    ) WITH CLUSTERING ORDER BY (utc_timestamp ASC);
"""

class CassandraDB:

    def __init__(self, cassandra_cluster: str, keyspacename: str = "stock") -> None:
        auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        cluster = Cluster([cassandra_cluster], port=9042, auth_provider=auth_provider)
        self.session = cluster.connect()
        self.session.set_keyspace(keyspacename)
        self.session.execute(DB_TABLESTR)
        print("Finish initializing the table")

    def write_databack(self, db_str: str):
        """Writet the database with the db_str"""
        try:
            self.session.execute(db_str)
        except:
            raise Exception("Write back failed")
