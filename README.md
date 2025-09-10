# Real-Time Stocks Market Data Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)

---

## 📌 Project Overview
This project demonstrates an **end-to-end real-time data pipeline** using the **Modern Data Stack**.  
We capture **live stock market data** from an external API, stream it in real time, orchestrate transformations, and deliver analytics-ready insights — all in one unified project.

![Architecture (1)](https://github.com/user-attachments/assets/6b49eb4d-4bf7-473d-9281-50c20b241760)


---

## ⚡ Tech Stack
- **Snowflake** → Cloud Data Warehouse  
- **DBT** → SQL-based Transformations  
- **Apache Airflow** → Workflow Orchestration  
- **Apache Kafka** → Real-time Streaming  
- **Python** → Data Fetching & API Integration  
- **Docker** → Containerization  
- **Power BI** → Data Visualization  

---

## ✅ Key Features
- Fetching **live stock market data** (not simulated) from an API  
- Real-time streaming pipeline with **Kafka**  
- Orchestrated ETL workflow using **Airflow**  
- Transformations using **DBT** inside Snowflake  
- Scalable cloud warehouse powered by **Snowflake**  
- Analytics-ready **Power BI dashboards**  

---

## 📂 Repository Structure
real-time-stocks-pipeline/
├── producer/                     # Kafka producer (Finnhub API)
│   └── stock_producer.py
├── consumer/                     # Kafka consumer (MinIO sink)
│   └── stock_consumer.py
├── dbt_stocks/models/
│   ├── bronze
│   │   ├── bronze_stg_stock_quotes.sql
│   │   └── sources.yml
│   ├── silver
│   │   └── silver_clean_stock_quotes.sql
│   └── gold
│   │   ├── gold_candlestick.sql
│   │   ├── gold_kpi.sql
│   │   └── gold_treechart.sql
├── dag/
│   └── minio_to_snowflake.py
├── docker-compose.yml            # Kafka, Zookeeper, MinIO, Airflow, Postgres
├── requirements.txt
│── README.md # Documentation

---

## 🚀 Getting Started
1. Clone this repo and set up environment  
2. Start Kafka + Airflow services via Docker  
3. Run the Python producer to fetch live stock data  
4. Data flows into Snowflake → DBT applies transformations  
5. Orchestrate everything with Airflow  
6. Connect Power BI for visualization  

---

## 📊 Final Deliverables
- **Automated real-time data pipeline**  
- **Snowflake tables (Bronze → Silver → Gold)**  
- **Transformed analytics models with DBT**  
- **Orchestrated DAGs in Airflow**  
- **Power BI dashboard with live insights**  

---

**Author**: *Jaya Chandra Kadiveti* 

**LinkedIn**: [username](https://www.linkedin.com/in/jayachandrakadiveti/) 

**Contact**: [Kadivetijayachandra@gmail.com](mailto:Kadivetijayachandra@gmail.com)
