FROM apache/superset:1.3.0
USER root
RUN pip install "trino[sqlalchemy]" sqlalchemy-trino
COPY docker-init.sh /app/docker-init.sh
RUN chmod +x /app/docker-init.sh
USER superset
ENTRYPOINT ["/app/docker-init.sh"]