

## 1. Структура модуля **`Batching`**

```
ecommerce_platform/
├── batching/
│   ├── dags/                           # Будет монтироваться как том
│   │   ├── orchestrate_realtime_ingestion.py
│   │   └── orchestrate_daily_data_refinement.py
│   ├── etl/
│   │   ├── streaming/
│   │   │   └── ingest_kafka_to_iceberg.py  # Бывший main.py
│   │   └── batch/
│   │       └── refine_and_backfill_iceberg.py # Бывший batch_processing_spark.py
│   ├── config/
│   │   └── spark_common_config.py      # Для общих настроек Spark
│   ├── logs/                           # Будет монтироваться как том
│   └── Dockerfile                      # Dockerfile для сборки образа Airflow
├── docker-compose.yml                  # Основной docker-compose файл
```

| Зависимые компоненты модуля |
| :------------------------------------ |
| clickhouse |
| zookeeper |
| kafka |
| minio |
| spark-master |
| spark-worker |
| airflow-db |
| airflow-init |
| airflow-webserver |
| airflow-scheduler |

## 2. Принцип работы **`Batching`**

- Потоковая обработка (Streaming) — принимает данные из Kafka и записывает их в ClickHouse (через Iceberg на S3/MinIO) в режиме реального времени.
- Пакетная обработка (Batch) — запускается раз в сутки для уточнения данных, пришедших от стриминга, и восполняет недостающие данные в случае падения стриминга, также записывая их в ClickHouse (через Iceberg на S3/MinIO).


| Имя файла DAG/Скрипта                 | Назначение                                             |
| :------------------------------------ | :----------------------------------------------------- |
| `orchestrate_realtime_ingestion.py`   | Управляет запуском задания по потоковой загрузке данных. |
| `orchestrate_daily_data_refinement.py`| Ежедневно уточняет данные и восполняет недостающие.     |
| `ingest_kafka_to_iceberg.py`          | Загружает данные из Kafka в Iceberg.                    |
| `refine_and_backfill_iceberg.py`      | Уточняет данные и восполняет недостающие записи в Iceberg. |

