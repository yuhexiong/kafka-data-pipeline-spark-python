# Kafka Data Pipeline Spark

Data pipeline written by Spark to transfer kafka to kakfa, doris.  

## Overview

- Language: Python
- Data Processing Framework: Spark v3.5.1


## Run

### Run Docker Container
```
docker build -t my-spark-app .
docker run --rm my-spark-app
```


## Entry

### 1. Kafka To Kafka

code refer to [kafka_to_kafka.py](kafka_to_kafka.py)  

source_topic in host:port -> sink_topic in host:port   

### 2. Kafka To Doris

code refer to [kafka_to_doris.py](kafka_to_doris.py)  

- Kafka Data Structure
```
{
    "device_id": "FD2023",
    "device_name": "Drilling Machine 2000",
    "manufacturer": "TechTools Inc.",
    "model": "DTM-2000",
    "description": "Advanced drilling machine for industrial use",
    "location": "Production Line A"
}
```

- doris table

| device_id        | device_name          | manufacturer    | model     | description                                 | location          |
|------------------|----------------------|-----------------|-----------|---------------------------------------------|-------------------|
| "FD2023"         | "Drilling Machine 2000" | "TechTools Inc." | "DTM-2000" | "Advanced drilling machine for industrial use" | "Production Line A" |


### 3. Kafka List To Doris

code refer to  
(1) [kafka_list_to_doris.py](kafka_list_to_doris.py)  
(2) define schema in yaml [kafka_list_to_doris_with_yaml.py](kafka_list_to_doris_with_yaml.py) and [kafka_list_to_doris_setting.yaml](kafka_list_to_doris_setting.yaml)  

- Kafka Data Structure
```
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

- doris table

| id      | device_name           | timestamp           | manufacturer    | model     | description                                 | location          | battery_voltage |
|---------|-----------------------|---------------------|-----------------|-----------|---------------------------------------------|-------------------|-----------------|
| "FD2023"| "Drilling Machine 2000"| "2024-07-19T10:00:00Z" | "TechTools Inc." | "DTM-2000" | "Advanced drilling machine for industrial use" | "Production Line A"| 12.5            |


### 3. Kafka Cdc To Doris

after the debezium cdc tool uploads the changed data in the database to kafka, it processes its data format and transmits it to doris. do not care about the data before modification, only add the data after to doris, and use the overlay function of doris to directly replace it.  

code refer to  
(1) [kafka_cdc_to_doris.py](kafka_cdc_to_doris.py)  

- Kafka Data Structure
```
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

- doris table

| id      | device_name           | timestamp           | manufacturer    | model     | description                                 | location          | battery_voltage |
|---------|-----------------------|---------------------|-----------------|-----------|---------------------------------------------|-------------------|-----------------|
| "FD2023"| "Drilling Machine 2000"| "2024-07-19T10:00:00Z" | "TechTools Inc." | "DTM-2000" | "Advanced drilling machine for industrial use" | "Production Line A"| 12.5            |


