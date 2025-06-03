
# Инструкция по запуску Streaming


## 1. Инициализация и проверка логов
- Сервисы " " инициализируются в `ecommerce_platform-main/docker-compose.yml`
- Убедитесь, что все порты в `ecommerce_platform-main/streaming/app/Dockerfile.*service*` и в `ecommerce_platform-main/docker-compose.yml` взаимно однозначно сопоставлены.
- Проверка pull-модели consumer'ов (просмотр логов на ошибки и успешное подключение к Kafka):

```sh

docker logs -f kafka_to_clickhouse_w_postgres_stream
docker logs -f kafka_to_clickhouse_w_mongo_stream 
docker logs -f kafka_to_clickhouse_w_kafka_stream

```
## 2. Перезагрузка (при необходимости)
- В случае несоответствий перезагрузите контейнеры:

```sh
docker-compose down 
docker-compose up -d --build

```

## 3. Проверка работы kafka_generator 

```sh
docker logs -f kafka_generator

```

## 4. Проверка передачи данных в Clickhouse:

```sh
docker logs -f clickhouse

docker exec -it clickhouse clickhouse-client

SELECT count(*) FROM d_customers;

SELECT * FROM d_products ORDER BY load_ts DESC LIMIT 10;

SELECT * FROM d_orders;

```


## 5. Имитация падения

```sh
## 1. Откройте 1 терминал и запустите генератор: 

docker compose -f docker-compose.yml up -d kafka_generator

## 2. Откройте логи генератора:

docker logs -f kafka_generator

## 3. Откройте 2 терминал и запустите стриминг:
docker compose -f docker-compose.yml up -d kafka_to_clickhouse_w_postgres_stream

## 4. Откройте его логи:
docker logs -f kafka_to_clickhouse_w_postgres_stream


## 5. "Убейте" контейнер стриминга:
docker compose -f docker-compose.yml stop kafka_to_clickhouse_w_postgres_stream

## 6. Получаем имя контейнера Kafka-брокера:

KAFKA_BROKER_CONTAINER="kafka_infra"

## 7. Проверяем смещения для вашей группы и топика
docker exec ${KAFKA_BROKER_CONTAINER} kafka-consumer-groups --bootstrap-server kafka:9092 --group clickhouse_consumer_group_01 --describe --members --verbose


## 8. Перезапуск и восстановление:

docker compose -f docker-compose.yml start kafka_to_clickhouse_w_postgres_stream

## 9. Просмотр логов:

docker logs -f kafka_to_clickhouse_w_postgres_stream


## Стриминг продолжит с того смещения, которое было последний раз закоммичено в Kafka.

```
