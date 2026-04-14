FROM ghcr.io/mlflow/mlflow:v3.11.1

USER root

RUN apt-get update && \
    apt-get install -y \
      curl \
      gcc \
      python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    boto3>=1.34.69 \
    pyarrow>=15.0.2 \
    protobuf>=5.0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/entrypoint.sh"]
