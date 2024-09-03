# Kafka Data Pipeline Spark

Data pipeline written by Spark to transfer Kafka to Kafka, Doris.  

## Overview

- Language: Python
- Data Processing Framework: Spark v3.5.1


## Run

### Run Docker Container

edit filename in docker-compose.yaml  
```
docker compose up -d
```


## Entry

### 1. Kafka To Kafka

code refer to [kafka_to_kafka.py](kafka_to_kafka.py)  

source_topic in host:port -> sink_topic in host:port   

### 2. Kafka To Doris

code refer to [kafka_to_doris.py](kafka_to_doris.py)  

- Kafka Data Structure
```json
{
    "device_id": "FD2023",
    "device_name": "Drilling Machine 2000",
    "manufacturer": "TechTools Inc.",
    "model": "DTM-2000",
    "description": "Advanced drilling machine for industrial use",
    "location": "Production Line A"
}
```

- Doris table

| device_id        | device_name          | manufacturer    | model     | description                                 | location          |
|------------------|----------------------|-----------------|-----------|---------------------------------------------|-------------------|
| "FD2023"         | "Drilling Machine 2000" | "TechTools Inc." | "DTM-2000" | "Advanced drilling machine for industrial use" | "Production Line A" |


### 3. Kafka List To Doris

code refer to  
(1) [kafka_list_to_doris.py](kafka_list_to_doris.py)  
(2) define schema in yaml [kafka_list_to_doris_with_yaml.py](kafka_list_to_doris_with_yaml.py) and [kafka_list_to_doris_setting.yaml](kafka_list_to_doris_setting.yaml)  

- Kafka Data Structure
```json
{
    "payload": [
        {
            "device_id": "FD2023",
            "device_name": "Drilling Machine 2000",
            "timestamp": "2024-07-19T10:00:00Z",
            "manufacturer": "TechTools Inc.",
            "model": "DTM-2000",
            "description": "Advanced drilling machine for industrial use",
            "location": "Production Line A",
            "battery_voltage": "12.5"
        }
    ]
}
```

- Doris table

| id      | device_name           | timestamp           | manufacturer    | model     | description                                 | location          | battery_voltage |
|---------|-----------------------|---------------------|-----------------|-----------|---------------------------------------------|-------------------|-----------------|
| "FD2023"| "Drilling Machine 2000"| "2024-07-19T10:00:00Z" | "TechTools Inc." | "DTM-2000" | "Advanced drilling machine for industrial use" | "Production Line A"| 12.5            |


### 4. Kafka Cdc To Doris

after the debezium cdc tool uploads the changed data in the database to kafka, it processes its data format and transmits it to doris. do not care about the data before modification, only add the data after to doris, and use the overlay function of doris to directly replace it.  

code refer to  
(1) [kafka_cdc_to_doris.py](kafka_cdc_to_doris.py)  
(2) define schema in yaml [kafka_cdc_to_doris_with_yaml.py](kafka_cdc_to_doris_with_yaml.py) and [kafka_cdc_to_doris_setting.yaml](kafka_cdc_to_doris_setting.yaml)  


- Kafka Data Structure
```json
{
    "payload": {
        "before": null,
        "after": {
            "id": "FD2023",
            "device_name": "Drilling Machine 2000",
            "timestamp": 1721383200000,
            "manufacturer": "TechTools Inc.",
            "model": "DTM-2000",
            "description": "Advanced drilling machine for industrial use",
            "location": "Production Line A",
            "battery_voltage": 12.5
        }
    }
}
```

- Doris table

| id      | device_name           | timestamp           | manufacturer    | model     | description                                 | location          | battery_voltage |
|---------|-----------------------|---------------------|-----------------|-----------|---------------------------------------------|-------------------|-----------------|
| "FD2023"| "Drilling Machine 2000"| "2024-07-19T10:00:00Z" | "TechTools Inc." | "DTM-2000" | "Advanced drilling machine for industrial use" | "Production Line A"| 12.5            |


### 5. 2 Kafka To Kafka

code refer to [2kafka_to_kafka.py](2kafka_to_kafka.py)  

- Kafka Data Structure Device Info
```json
{
    "payload": [
        {
            "id": "FD2023",
            "device_name": "Drilling Machine 2000",
            "manufacturer": "TechTools Inc.",
            "model": "DTM-2000",
            "description": "Advanced drilling machine for industrial use",
            "location": "Production Line A",
            "battery_voltage": "12.5"
        }
    ]
}
```

- Kafka Data Structure Device Status
```json
{
    "device_id": "FD2023",
    "timestamp": "2024-07-19T10:05:00Z",
    "status": "active"
}
```

- Kafka Data Structure After Join
```json
{
    "id": "FD2023",
    "device_name": "Drilling Machine 2000",
    "manufacturer": "TechTools Inc.",
    "model": "DTM-2000",
    "description": "Advanced drilling machine for industrial use",
    "location": "Production Line A",
    "battery_voltage": "12.5",
    "timestamp": "2024-07-19T10:05:00Z",
    "status": "active"
}
```

### 6. 2 Kafka To Doris

code refer to [2kafka_to_doris.py](./2kafka_to_doris.py)  
data source same as `5. 2 Kafka To Kafka`  

- Doris table

| id     | device_name          | manufacturer  | model   | description                               | location        | battery_voltage | timestamp           | status |
|--------|----------------------|---------------|---------|-------------------------------------------|-----------------|-----------------|---------------------|--------|
| FD2023 | Drilling Machine 2000 | TechTools Inc.| DTM-2000| Advanced drilling machine for industrial use | Production Line A | 12.5            | 2024-07-19T10:05:00Z| active |
