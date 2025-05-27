import datetime
from web3 import Web3


def hex_timestamp_to_datetime(w3: Web3,hex_timestamp):
    try:
        unix_timestamp = w3.to_int(hexstr=hex_timestamp)
        return datetime.datetime.fromtimestamp(unix_timestamp)
    except ValueError:
        return "Invalid hexadecimal timestamp"