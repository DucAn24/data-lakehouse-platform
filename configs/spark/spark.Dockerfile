FROM apache/spark:4.0.0-python3

USER root

# Install additional packages
RUN apt-get update && \
    apt-get install -y curl wget && \
    rm -rf /var/lib/apt/lists/*

# Add AWS dependencies for S3/MinIO support
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.470.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.470/aws-java-sdk-bundle-1.12.470.jar && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar

# Iceberg dependencies (commented out - using Delta Lake instead)
# RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.13-1.5.2.jar \
#     https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/1.5.2/iceberg-spark-runtime-3.5_2.13-1.5.2.jar

# Set proper permissions  
RUN chown -R spark:spark /opt/spark/jars

USER spark

# Environment variables for S3/MinIO
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=password123
ENV S3_ENDPOINT=http://minio:9000
ENV S3_PATH_STYLE_ACCESS=true

# Iceberg configuration (commented out - using Delta Lake instead)
# ENV SPARK_CONF_spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# ENV SPARK_CONF_spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
# ENV SPARK_CONF_spark.sql.catalog.spark_catalog.type=hive
# ENV SPARK_CONF_spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
# ENV SPARK_CONF_spark.sql.catalog.local.type=hadoop
# ENV SPARK_CONF_spark.sql.catalog.local.warehouse=s3a://warehouse/
