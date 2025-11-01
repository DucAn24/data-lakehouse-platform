FROM apache/spark:3.5.5

USER root

RUN apt-get update && \
    apt-get install -y curl wget && \
    rm -rf /var/lib/apt/lists/*

# Add AWS dependencies 
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Add Delta Lake 
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.1.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.2.1.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

# Add Kafka 
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# Add Hive Metastore 
RUN curl -L -o /opt/spark/jars/hive-metastore-3.1.3.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar && \
    curl -L -o /opt/spark/jars/hive-exec-3.1.3.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar && \
    curl -L -o /opt/spark/jars/hive-common-3.1.3.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-common/3.1.3/hive-common-3.1.3.jar && \
    curl -L -o /opt/spark/jars/hive-serde-3.1.3.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-serde/3.1.3/hive-serde-3.1.3.jar && \
    curl -L -o /opt/spark/jars/libfb303-0.9.3.jar \
    https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar && \
    curl -L -o /opt/spark/jars/postgresql-42.7.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar

# Install Python dependencies 
RUN pip install --no-cache-dir \
    delta-spark==3.2.1 

RUN mkdir -p /opt/spark/jobs && \
    chown -R spark:spark /opt/spark/jars && \
    chown -R spark:spark /opt/spark/jobs

USER spark

# Environment variables for S3/MinIO
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=password123
ENV S3_ENDPOINT=http://minio:9000
ENV S3_PATH_STYLE_ACCESS=true

# Delta Lake configuration
ENV SPARK_CONF_spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
ENV SPARK_CONF_spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
