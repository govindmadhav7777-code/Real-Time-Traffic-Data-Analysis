# Real-Time-Traffic-Data-Analysis
# 🚦 Real-Time Traffic Data Lakehouse

A real-time **Data Engineering pipeline** for ingesting, processing, cleaning, and analyzing traffic data using **Apache Kafka, Apache Spark Structured Streaming, Delta Lake, Hive Metastore, PostgreSQL, and Docker**.

The project follows the **Medallion Architecture**:

**Bronze → Silver → Gold**

---

## 🏗️ Architecture

```text
                ┌──────────────────┐
                │ Traffic Producer │
                │     Python       │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │      Kafka       │
                │  traffic_events  │
                └────────┬─────────┘
                         │
                         ▼
             ┌───────────────────────┐
             │   Spark Structured    │
             │      Streaming        │
             └───────────┬───────────┘
                         │
                         ▼
                ┌──────────────────┐
                │  Bronze Layer    │
                │   Raw Delta      │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │   Silver Layer   │
                │ Cleaned + Valid  │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │    Gold Layer    │
                │ Analytics Tables │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │ Hive Metastore   │
                │   + PostgreSQL   │
                └──────────────────┘
```

---

## ✨ Features

* Real-time traffic event ingestion using **Kafka**
* Stream processing using **Spark Structured Streaming**
* ACID storage using **Delta Lake**
* Bronze → Silver → Gold **Medallion Architecture**
* Data validation and cleaning
* Watermarking and duplicate handling
* Traffic peak-hour detection
* Speed classification
* Traffic zone classification
* Road dimension generation
* Fact table generation
* Hive Metastore for metadata management
* PostgreSQL-backed Hive Metastore
* Fully containerized using **Docker Compose**

---

## 🥉 Bronze Layer

The Bronze layer stores the raw traffic events received from Kafka.

### Responsibilities

* Consume events from Kafka
* Preserve raw event data
* Convert incoming data into Delta format
* Provide a reliable source for downstream processing

Example location:

```text
/opt/spark/warehouse/traffic_bronze
```

---

## 🥈 Silver Layer

The Silver layer transforms raw Bronze data into clean and validated traffic data.

### Processing

* Data type validation
* Missing-value handling
* Speed validation
* Timestamp validation
* Watermarking
* Duplicate removal
* Peak-hour identification
* Speed-band classification

Example derived fields:

```text
hour
peak_flag
speed_band
```

Output:

```text
/opt/spark/warehouse/traffic_silver
```

---

## 🥇 Gold Layer

The Gold layer contains analytics-ready tables built from the Silver layer.

### Dimension Tables

#### `dim_zone`

Contains information about traffic zones.

```text
city_zone
zone_type
traffic_risk
```

Example zone types:

```text
CBD         → Commercial
TECHPARK    → IT HUB
AIRPORT     → Transit Hub
TRAINSTATION→ Transit Hub
```

#### `dim_road`

Contains road-level information.

```text
road_id
road_type
speed_limit
```

### Fact Table

#### `fact_traffic`

Contains traffic events prepared for analytics.

```text
vehicle_id
road_id
city_zone
speed_int
congestion_level
event_ts
peak_flag
speed_band
hour
weather
date
```

---

## 🛠️ Tech Stack

| Technology                 | Purpose                           |
| -------------------------- | --------------------------------- |
| Python                     | Data generation and pipeline code |
| Apache Kafka               | Real-time event streaming         |
| Apache Spark               | Distributed stream processing     |
| Spark Structured Streaming | Real-time processing              |
| Delta Lake                 | Lakehouse storage                 |
| Hive Metastore             | Metadata management               |
| PostgreSQL                 | Hive Metastore database           |
| Docker                     | Containerization                  |
| Docker Compose             | Multi-container orchestration     |
| SQL                        | Data querying and analytics       |

---

## 📁 Project Structure

```text
Real-Time-Traffic-Data-Analysis/
│
├── apps/
│   ├── traffic_bronze.py
│   ├── traffic_silver.py
│   └── traffic_gold.py
│
├── producer/
│   └── ...
│
├── hive-conf/
│   └── hive-site.xml
│
├── warehouse/
│   └── Generated locally
│
├── spark-ivy/
│   └── Generated locally
│
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

> `warehouse/` and `spark-ivy/` contain generated/local data and are excluded from Git.

---

## 🔐 Environment Variables

Create a `.env` file in the project root:

```env
POSTGRES_DB=metastore
POSTGRES_USER=hive
POSTGRES_PASSWORD=your_strong_password
```

The `.env` file is intentionally excluded from Git to prevent credentials from being committed.

For other developers, provide a `.env.example` file containing only placeholder values.

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/govindmadhav7777-code/Real-Time-Traffic-Data-Analysis.git

cd Real-Time-Traffic-Data-Analysis
```

### 2. Create `.env`

```env
POSTGRES_DB=metastore
POSTGRES_USER=hive
POSTGRES_PASSWORD=your_strong_password
```

Make sure the PostgreSQL password matches the password configured for the Hive Metastore connection.

### 3. Validate Docker Compose

```bash
docker compose config
```

If there are no errors, start the complete stack:

```bash
docker compose up -d
```

### 4. Check running containers

```bash
docker ps
```

The main services include:

```text
hive-metastore-db
hive-metastore
spark-master
spark-worker
kafka
kafka-ui
```

---

## 📊 Services & Ports

| Service         |    Port |
| --------------- | ------: |
| Spark Master UI |  `8080` |
| Spark Worker UI |  `8081` |
| Spark Master    |  `7077` |
| Hive Metastore  |  `9083` |
| Kafka           |  `9092` |
| Kafka External  | `29092` |
| Kafka UI        |  `8090` |
| PostgreSQL      |  `5435` |

Kafka UI can be accessed locally at:

```text
http://localhost:8090
```

Spark Master UI:

```text
http://localhost:8080
```

---

## ▶️ Running the Pipeline

### Start the infrastructure

```bash
docker compose up -d
```

### Run Bronze

```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_bronze.py
```

### Run Silver

```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_silver.py
```

### Run Gold

```bash
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark-apps/traffic_gold.py
```

---

## 🧪 Monitoring

### Spark

Open:

```text
http://localhost:8080
```

to monitor the Spark cluster.

### Kafka UI

Open:

```text
http://localhost:8090
```

to inspect Kafka topics, messages, partitions, and consumer activity.

### Hive Metastore

Hive Metastore runs on:

```text
localhost:9083
```

---

## 🗄️ Data Storage

The pipeline stores Delta Lake data locally:

```text
warehouse/
├── traffic_bronze/
├── traffic_silver/
├── dim_zone/
├── dim_road/
└── fact_traffic/
```

Streaming checkpoints are also stored locally.

These generated files are intentionally excluded from version control.

---

## 🔒 Security Notes

This project is designed primarily for **local development and learning**.

The Docker configuration currently uses plaintext Kafka communication and exposes several services to the host machine.

For production deployment, the following should be implemented:

* Kafka authentication and TLS
* PostgreSQL authentication and restricted networking
* Secret management
* Network isolation
* HTTPS/TLS where applicable
* Production-grade Spark configuration
* Cloud object storage such as S3/ADLS/GCS
* Proper access control and monitoring

Never commit real passwords, API keys, tokens, private keys, or other secrets to Git.

---

## 🎯 Project Goals

This project demonstrates practical Data Engineering concepts including:

* Real-time data ingestion
* Event streaming
* Distributed processing
* Data lakehouse architecture
* Medallion architecture
* Data quality
* Stream processing
* Delta Lake
* Data modeling
* Dimensional modeling
* Containerized infrastructure
* Metadata management

---

## 📌 Future Improvements

Potential extensions include:

* Apache Airflow orchestration
* Databricks deployment
* Cloud storage integration
* Power BI dashboard
* Real-time traffic analytics dashboard
* Advanced congestion prediction
* ML-based traffic forecasting
* Automated data-quality monitoring
* CI/CD pipeline
* Kubernetes deployment

---

## 👨‍💻 Author

**Govind Madhav**

BTech — Artificial Intelligence & Data Science

This project was developed as a practical **Data Engineering / Lakehouse project** using modern streaming and distributed-data technologies.
