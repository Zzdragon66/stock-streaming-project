from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
import json
import argparse

class SparkStreamProcessor():

    # Define the schema of the JSON key
    key_schema = StructType([
        StructField("stock", StringType(), True),
    ])
    # Define the schema of the JSON value
    value_schema = StructType([
        StructField("curr_price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("n_transactions", IntegerType(), True),
        StructField("volume_cumulative", DoubleType(), True),
        StructField("utc_timestamp", LongType(), True)
    ])

    def __init__(self, spark_cluster : str, kafka_cluster : str, 
                 topic : str, cassandra_cluster : str,
                 keyspace : str, table_name : str,
                 cassandra_user : str, cassandra_pwd : str, n_nodes = 1):
        self.spark = SparkSession.builder \
            .appName(f"SparkStreamProcessing-{topic}") \
            .config("master", spark_cluster) \
            .config("spark.cassandra.connection.host", cassandra_cluster) \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", cassandra_user) \
            .config("spark.cassandra.auth.password", cassandra_pwd) \
            .getOrCreate()
        self.topic = topic
        self.cassandra_user, self.cassandra_pwd = cassandra_user, cassandra_pwd
        self.cassandra_cluster = cassandra_cluster
        self.keyspace, self.table_name  = keyspace, table_name
        self.kafka_cluster = kafka_cluster
        self.n_nodes = n_nodes
    
    def write_to_cassandra(self, df, epoch_id):
        df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", self.keyspace) \
        .option("table", self.table_name) \
        .save()


    def process_data(self):
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_cluster) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()
        # parse the table based on the table definition
        parsed_df = kafka_df \
        .select(
            from_json(col("key").cast("string"), SparkStreamProcessor.key_schema).alias("parsed_key"),
            from_json(col("value").cast("string"), SparkStreamProcessor.value_schema).alias("parsed_value")
        ) \
        .select("parsed_key.*", "parsed_value.*")
        # write table to cassandra
        query = parsed_df \
            .writeStream \
            .foreachBatch(self.write_to_cassandra) \
            .start()
        
        query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_cluster", type=str, required=True)
    parser.add_argument("--kafka_cluster", type=str, required=True)
    parser.add_argument("--cassandra_cluster", required=True, type=str)
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--keyspace", type=str, required=True)
    parser.add_argument("--table_name", type=str, required=True)
    parser.add_argument("--cassandra_user", type=str, required=True)
    parser.add_argument("--cassandra_pwd", type=str, required=True)
    args = parser.parse_args()
    processor = SparkStreamProcessor(
        spark_cluster=args.spark_cluster,
        kafka_cluster=args.kafka_cluster,
        topic = args.topic,
        keyspace = args.keyspace, table_name=args.table_name,
        cassandra_cluster=args.cassandra_cluster,
        cassandra_user=args.cassandra_user, cassandra_pwd=args.cassandra_pwd
    )
    processor.process_data()