FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
  apt-get install -y \
  curl \
  wget \
  openjdk-17-jre-headless && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
  --constraint "${CONSTRAINT_URL}" \
  apache-airflow-providers-amazon \
  apache-airflow-providers-apache-kafka \
  apache-airflow-providers-trino \
  apache-airflow-providers-postgres \
  apache-airflow-providers-http


RUN pip install --no-cache-dir \
  boto3 \
  psycopg2-binary \
  mlflow-skinny \
  pyspark==3.5.3 \
  python-dotenv
