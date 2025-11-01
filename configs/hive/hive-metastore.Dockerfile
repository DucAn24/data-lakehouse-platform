FROM apache/hive:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Download JDBC driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/hive/lib/postgresql-jdbc.jar

# Download AWS SDK and Hadoop AWS JARs 
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar

USER hive

WORKDIR /opt/hive