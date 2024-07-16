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

