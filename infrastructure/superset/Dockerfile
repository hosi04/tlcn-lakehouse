FROM apache/superset:4.0.2

USER root

RUN apt-get update && \
    apt-get install -y \
      postgresql-client \
      curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "trino[sqlalchemy]" sqlalchemy-trino psycopg2-binary

COPY superset_config.py /app/pythonpath/superset_config.py

COPY docker-init.sh /app/docker-init.sh
RUN chmod +x /app/docker-init.sh

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
ENV PYTHONPATH=/app/pythonpath:$PYTHONPATH

ENTRYPOINT ["/app/docker-init.sh"]