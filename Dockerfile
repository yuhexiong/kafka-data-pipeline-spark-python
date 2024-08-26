FROM apache/spark:3.5.1

USER root

# Install Python
RUN apt-get update --allow-releaseinfo-change && \
    apt-get install -y python3 python3-pip

COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

# Install Kafka
RUN wget -qO /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -qO /opt/spark/jars/kafka-clients-2.8.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar && \
    wget -qO /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -qO /opt/spark/jars/commons-pool2-2.11.0.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.0/commons-pool2-2.11.0.jar

# Install Doris
RUN wget -qO /opt/spark/jars/spark-doris-connector-3.3_2.12-1.3.1.jar https://repo1.maven.org/maven2/org/apache/doris/spark-doris-connector-3.3_2.12/1.3.1/spark-doris-connector-3.3_2.12-1.3.1.jar  

# Install JDBC
RUN curl -o /opt/spark/jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar


# mkdir checkpoint and chmod
RUN mkdir -p /app/checkpoint && \
    chmod -R 777 /app/checkpoint