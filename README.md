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

### 1. kafka_to_kafka

source_topic in host:port -> sink_topic in host:port   

### 2. kafka_to_doris

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



