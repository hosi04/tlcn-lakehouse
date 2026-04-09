FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
  apt-get install -y \
  curl \
  wget && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

USER airflow

# Dùng constraint file của Airflow 2.9.3 để tránh xung đột dependency
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
  mlflow-skinny
