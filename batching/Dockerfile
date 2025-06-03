# Dockerfile
# Расположение: ecommerce_platform/batching/Dockerfile

FROM apache/airflow:2.9.2-python3.9

ARG AIRFLOW_VERSION="2.9.2"
ARG PYTHON_VERSION="3.9"
ARG SPARK_VERSION="3.5.0"
ARG HADOOP_VERSION="3"

RUN curl -SsL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" -o "/tmp/constraints.txt"

RUN pip install "apache-airflow-providers-apache-spark==4.8.1" \
    --constraint "/tmp/constraints.txt"

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    openjdk-17-jre-headless \
    curl \
    gnupg \
    tar \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN mkdir -p ${SPARK_HOME} && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xzC ${SPARK_HOME} --strip-components=1 && \
    chown -R airflow:0 ${SPARK_HOME} && \
    chmod -R g+w ${SPARK_HOME}

USER airflow

RUN rm -rf /home/airflow/.ivy2 /home/airflow/.m2