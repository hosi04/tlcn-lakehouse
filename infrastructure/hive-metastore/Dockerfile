FROM apache/hive:3.1.3

USER root

RUN apt-get update && \
    apt-get install -y \
      netcat \
      postgresql-client \
      curl \
      wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/hive/scripts

COPY init-metastore.sh /opt/hive/scripts/init-metastore.sh
RUN chmod +x /opt/hive/scripts/init-metastore.sh

USER hive

ENTRYPOINT ["/opt/hive/scripts/init-metastore.sh"]