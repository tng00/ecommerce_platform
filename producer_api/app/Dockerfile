FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5001
# Переменная окружения для адреса Kafka, будет передана из docker-compose
ENV KAFKA_BROKER_URL=kafka:9092

EXPOSE 5001

CMD ["flask", "run"]