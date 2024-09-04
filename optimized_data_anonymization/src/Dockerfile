# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
ENV SPARK_VERSION=3.2.4 \
    HADOOP_VERSION=3.2 \
    JAVA_VERSION=11

# Install Java and dependencies in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    openjdk-${JAVA_VERSION}-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark in one layer
RUN curl -o spark.tgz -L "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf spark.tgz -C /opt/ && \
    rm spark.tgz && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install the required Python packages
RUN pip install --no-cache-dir pandas faker pyspark

# Copy the application code
COPY ./src /app/src
WORKDIR /app/src

# Ensure the right permissions (optional, if needed)
# RUN chown -R nobody:nogroup /app

# Command to run the application
CMD ["python", "main.py", "--target_size_mb", "100", "--output_file", "anonymized_data.csv"]
