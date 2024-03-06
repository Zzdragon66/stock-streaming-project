import sys

def dict_to_str(dict_in):
    env_write_str = ""
    format_str = "{key}={value}\n"
    for k, v in dict_in.items():
        env_write_str += format_str.format(key=k, value=v)
    return env_write_str

def get_dockerhub():
    """Get the dockerhub username and password 
    """
    dockerhub_username = input("Enter the dockerhub username: ")
    dockerhub_password = input("Enter the dockerhub password: ")
    return {
        "dockerhub_username" : dockerhub_username,
        "dockerhub_password" : dockerhub_password
    }

def get_api_key():
    """Get the stock market api key"""
    api_key = input("Enter the API key for stock market: ")
    return {
        "stock_api" : api_key
    }

def get_reddit_api():
    "Get the reddit api key"
    reddit_client = input("Enter the reddit client: ")
    reddit_secret = input("Enter the reddit secret: ")
    return {
        "reddit_client" : reddit_client, 
        "reddit_secret" : reddit_secret
    }

def main():
    #TODO(Allen): Add the reddit client id and secret
    env_write_dict = {}
    env_write_dict.update(get_dockerhub())
    env_write_dict.update(get_api_key())
    env_write_dict.update(get_reddit_api())
    env_str = dict_to_str(env_write_dict)
    with open(".env", "w") as f:
        f.write(env_str)

if __name__ == "__main__":
    main()