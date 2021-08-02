FROM --platform=linux/arm/v8 ubuntu:20.04

# A versão ARM está com um bug desconhecido. Será investigado posteriormente.
# Error: Could not find or load main class org.apache.spark.launcher.Main
# Caused by: java.lang.ClassNotFoundException: org.apache.spark.launcher.Main
# /opt/spark-3.1.2-bin-hadoop3.2/bin/spark-class: line 96: CMD: bad array subscript

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update \
  && apt upgrade -y \
  && apt install -y openjdk-11-jre python3 python3-pip curl \
  && apt clean

RUN pip3 install -q pyspark

ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2

RUN curl -o /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz https://mirror.nbtelecom.com.br/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  && tar -xvzf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} \
  && rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

WORKDIR /app

COPY wordcount.py /app
COPY pedidos.py /app
COPY run.sh /app

ENTRYPOINT [ "bash", "/app/run.sh" ]