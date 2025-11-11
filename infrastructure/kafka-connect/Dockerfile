# kafka connector
FROM confluentinc/cp-kafka-connect:7.3.0

# debezium connector
RUN mkdir -p /usr/share/java/debezium-connector && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/1.7.0.Final/debezium-connector-mysql-1.7.0.Final-plugin.tar.gz && \
    tar -xvf debezium-connector-mysql-1.7.0.Final-plugin.tar.gz -C /usr/share/java/debezium-connector && \
    rm debezium-connector-mysql-1.7.0.Final-plugin.tar.gz