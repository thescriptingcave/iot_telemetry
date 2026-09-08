#!/bin/bash
# Download JARs needed for Hive to connect to MinIO and Postgres

echo "Downloading JARs..."

# Use Docker's built-in tools to download JARs
apt-get update -qq && apt-get install -y -qq curl wget > /dev/null 2>&1 || true

# Download hadoop-aws
if [ ! -f /opt/hive/lib/hadoop-aws-3.3.6.jar ]; then
    wget -q -O /opt/hive/lib/hadoop-aws-3.3.6.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar
    echo "Downloaded hadoop-aws"
fi

# Download aws-java-sdk
if [ ! -f /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar ]; then
    wget -q -O /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
    echo "Downloaded aws-java-sdk"
fi

# Download postgresql JDBC
if [ ! -f /opt/hive/lib/postgresql-jdbc.jar ]; then
    wget -q -O /opt/hive/lib/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
    echo "Downloaded postgresql-jdbc"
fi

echo "JARs downloaded successfully"
