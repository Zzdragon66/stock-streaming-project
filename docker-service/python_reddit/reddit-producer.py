import praw
from datetime import datetime
import pandas as pd 
import argparse
from time import sleep
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

SUBREDDITS = ["trading", "stockmarket", "investing", "stocks", "wallstreetbets"]
ALL_FIELDS = ["title", "url", "subreddit", "id", "created_utc", "news"]
FIELDS = ["title", "url", "subreddit", "id"]

def init_reddit(reddit_client : str, reddit_secret : str):
    """Initialize the reddit instance"""
    return praw.Reddit(
        client_id=reddit_client,
        client_secret=reddit_secret,
        user_agent="macos:reddit_app_for_project"
    )

def init_cassandra(cassandra_cluster : str):
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster([cassandra_cluster], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS stock.reddit_news (
            id TEXT,
            subreddit TEXT,
            url TEXT,
            title TEXT,
            created_utc TIMESTAMP,
            news text,
            PRIMARY KEY (subreddit, created_utc, id)
        ) WITH CLUSTERING ORDER BY (created_utc DESC)
        """
    )
    return session

def reddit_stream(reddit, session):
    rows = []
    while True:
        recent_posts = reddit.subreddit("+".join(SUBREDDITS)).new(limit=100)
        for p in recent_posts:
            rows_dict = {field : str(getattr(p, field)) for field in FIELDS}
            rows_dict["created_utc"] = str(int(getattr(p, "created_utc") * 1000))
            rows.append(rows_dict)
        df = pd.DataFrame(rows)
        rows.clear()
        write_to_cassandra(df, session)
        print(f"Finish write at {datetime.now()}")
        sleep(60)
    

def write_to_cassandra(df, session):
    insert_cols = ", ".join(ALL_FIELDS)
    val_temp_str = """'{title}', '{url}', '{subreddit}', '{id}', {create_utc}, '{news}'"""
    for _, row in df.iterrows():
        val_cols = val_temp_str.format(title = row["title"].replace('\'', ""), url = row["url"], create_utc = row["created_utc"], subreddit=row["subreddit"], id=row["id"], news = "stock")
        insert_stmt = f"INSERT INTO stock.reddit_news ({insert_cols}) VALUES ({val_cols});"
        try:
            session.execute(insert_stmt)
        except:
            print(insert_stmt)
            continue

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--reddit_client", required=True, type=str)
    argparser.add_argument("--reddit_secret", required=True, type=str)
    argparser.add_argument("--cassandra_host", type=str, required=True)

    args = argparser.parse_args()
    reddit_instance = init_reddit(args.reddit_client, args.reddit_secret)
    cassandra = init_cassandra(args.cassandra_host)
    # start streaming
    reddit_stream(reddit_instance, cassandra)

if __name__ == "__main__":
    main()