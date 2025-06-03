# Start from a base image that includes Python, e.g., Python 3.9 slim
FROM python:3.11-slim-buster 

# Install Java Development Kit (JDK) - Spark requires Java
# Using a specific version (e.g., openjdk-11-jdk) is often good practice.
# You might need 'apt-transport-https' for some distros, but buster usually has it.
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \ 
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# Install PySpark and clickhouse-driver
# Use a version of PySpark compatible with your Spark cluster (Spark 3.5.0)
# pyspark[sql] installs both pyspark and the common SQL dependencies.
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    clickhouse-driver==0.2.6 # Ensure version is compatible, 0.2.6 is common

# Copy your ETL job script into the container
WORKDIR /app

COPY ../libs /app/libs/
COPY ./etl_job/etl_job.py .

# Command to run your ETL job (as defined in docker-compose)
CMD ["python", "etl_job.py"]