# 💹 Real-Time Cryptocurrency Analyzer with Azure

An end-to-end real-time data engineering project that captures, processes, and analyzes cryptocurrency prices using:

- 🔄 **Kafka** (Python Producer & Consumer)  
- ☁️ **Azure Data Lake Storage Gen2**  
- ⚡ **Azure Databricks** (PySpark transformations)  
- 🧩 **Azure Data Factory** (for orchestration)  


---

## 📌 Project Highlights

- Ingests real-time data from [CoinGecko API](https://www.coingecko.com/en/api)  
- Streams data via Apache Kafka  
- Stores raw JSON files to Azure Storage (date-wise folder structure)  
- Transforms JSON to Parquet using PySpark on Azure Databricks  
- Generates daily and overall crypto summary reports  
- Automates everything via Azure Data Factory  

---

## 🔐 Secrets Management

- Secret scope: `cryptoSecret`  
- Secure access to Azure Storage keys via Databricks secrets

---

## ⚙️ Tech Stack

| Layer           | Tool/Service              |
|----------------|---------------------------|
| Ingestion       | Apache Kafka, Python      |
| Streaming API   | CoinGecko API             |
| Cloud Storage   | Azure Data Lake Gen2      |
| Processing      | Azure Databricks (PySpark)|
| Orchestration   | Azure Data Factory        |

---

## 🔁 Workflow

1. **Kafka Producer** fetches real-time data from CoinGecko  
2. **Kafka Consumer** writes each record as JSON into ADLS `raw/` folder by date  
3. **Databricks Notebook 1** reads raw files, converts to daily Parquet under `processed/`  
4. **Databricks Notebook 2** creates daily summary in `output/daily_summary/`  
5. **Databricks Notebook 3** generates coin-wise metrics in `output/overall_summary/`  
6. **Azure Data Factory** pipelines automate steps 3–5 on a daily trigger  

---

## 📫 Author

- **Name**: Shreesaraan Devarajan
- **LinkedIn**: [linkedin.com/in/shreesaraan](https://www.linkedin.com/in/shreesaraan)  
- **GitHub**: [github.com/Shreesaraan](https://github.com/Shreesaraan)  

---

## 🔖 Tags

`#DataEngineering` `#Azure` `#Kafka` `#PySpark` `#StreamingData` `#PortfolioProject` `#CryptoAnalytics`
