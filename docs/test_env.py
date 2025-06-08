import os
from dotenv import load_dotenv

load_dotenv()
connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if connect_str:
    print("✅ Connection string loaded successfully.")
else:
    print("❌ Failed to load connection string.")
