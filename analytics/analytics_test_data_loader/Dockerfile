# analytics_etl/analytics_test_data_loader/Dockerfile
FROM python:3.11-slim-buster 

WORKDIR /app

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

RUN pip install --no-cache-dir clickhouse-driver==0.2.6 Faker

COPY analytics_test_data_loader.py .

CMD ["python", "analytics_test_data_loader.py"]