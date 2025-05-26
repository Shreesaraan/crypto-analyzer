# 💹 Real-Time Cryptocurrency Analyzer with Azure

This project is an **end-to-end real-time data engineering pipeline** that ingests cryptocurrency price data from the Coingecko API, processes it using Apache Kafka, stores it in **Azure Data Lake Gen2**, and analyzes it using **Azure Databricks with PySpark**.

---

## 🗂️ Project Structure

```bash
crypto-analyzer/
├── kafka/
│   ├── producer/
│   │   └── producer.py       # Fetches data from Coingecko API and pushes to Kafka
│   └── consumer/
│       └── consumer.py       # Consumes Kafka stream and writes to Azure Storage
├── azure/
│   └── storage_setup.md      # Documentation of Azure Storage setup
├── venv/                     # Python virtual environment (ignored in git)
├── requirements.txt          # Python dependencies
├── .gitignore
└── README.md
