'''
import json
import time
import requests
from kafka import KafkaProducer

API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=inr"
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'crypto_prices'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_data():
    response = requests.get(API_URL)
    return response.json()

if __name__ == "__main__":
    while True:
        data = fetch_data()
        timestamped = {
            "timestamp": int(time.time()),  # integer UNIX timestamp
            "prices": data
        }
        producer.send(TOPIC_NAME, value=timestamped)
        print(f"Sent: {timestamped}")
        time.sleep(15)  # Poll every 15 seconds
'''

import requests
import json
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=inr"
    try:
        response = requests.get(url)
        prices = response.json()
        return {
            "timestamp": time.time(),
            "prices": prices
        }
    except Exception as e:
        print("API Error:", e)
        return None

topic_name = "crypto-topic"

while True:
    data = fetch_crypto_data()
    if data and "status" not in data.get("prices", {}):
        producer.send(topic_name, value=data)
        print(f"Sent: {data}")
    else:
        print("Rate limited or invalid data. Skipping...")
    time.sleep(15)  # Respect API rate limit
