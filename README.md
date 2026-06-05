# Big Data IoT Pipeline: Kafka to HDFS

This project implements a real-time data pipeline that simulates IoT sensor data, streams it through Apache Kafka, and archives it persistently into an Apache Hadoop (HDFS) cluster.

## Architecture
The pipeline follows a distributed architecture:
1. **Producer**: Simulates IoT sensors (producer.py).
2. **Kafka**: Ingestion point for the sensor stream (iot_sensor_data topic).
3. **Tracker (Consumer)**: Reads data from Kafka (tracker.py) and manages persistent storage.
4. **HDFS**: Distributed file system for final storage.

## Setup Instructions

### 1. Launch Infrastructure
Deploy the Kafka (KRaft mode) and Hadoop (NameNode/DataNode) services using Docker Compose:
```bash
docker compose up -d
