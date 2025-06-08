'''
import json
from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import os
import dotenv

dotenv.load_dotenv()

bootstrap_servers = 'localhost:9092'
topic_name = 'crypto_prices'

connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = 'crypto-data'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Started consuming...")

for message in consumer:
    record = message.value

    # Skip Coingecko rate limit error responses (optional)
    if "status" in record.get("prices", {}):
        print("Rate limit hit, skipping...")
        continue

    timestamp = record.get('timestamp')
    prices = record.get('prices', {})

    if not timestamp or not prices:
        print("Invalid record, missing timestamp or prices, skipping...")
        continue

    # Convert Unix timestamp to datetime for folder and file naming
    dt = datetime.utcfromtimestamp(timestamp)

    date_str = dt.strftime("%Y-%m-%d")      # folder name by data time
    time_str = dt.strftime("%H-%M-%S")      # filename by data time

    # Blob name example: 2025-05-27/crypto_14-39-19.json
    blob_name = f"{date_str}/crypto_{time_str}.json"

    # Upload full record JSON as-is to Azure Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(record), overwrite=True)

    print(f"Uploaded {blob_name} to Azure")
'''

import json
from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import os
import dotenv

dotenv.load_dotenv()

# Azure config
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "crypto-data"

# Kafka config
topic_name = "crypto-topic"
bootstrap_servers = "localhost:9092"

# Initialize consumer and Azure client
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

print("Started consuming...")

for message in consumer:
    data = message.value

    now = datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")
    
    # Define blob path: raw/YYYY-MM-DD/crypto_HH-MM-SS.json
    blob_path = f"raw/{date_str}/crypto_{time_str}.json"
    
    try:
        blob_client = container_client.get_blob_client(blob_path)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        print(f"Uploaded to Azure: {blob_path}")
    except Exception as e:
        print(f"Failed to upload to Azure: {e}")
