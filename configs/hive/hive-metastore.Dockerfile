FROM apache/hive:4.0.0

# Download PostgreSQL JDBC driver
USER root

# Install curl first
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Download JDBC driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/hive/lib/postgresql-jdbc.jar

# Switch back to hive user
USER hive

# Set working directory
WORKDIR /opt/hive