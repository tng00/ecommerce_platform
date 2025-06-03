from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="realtime_spark_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # None для ручного запуска, вне теста  короткий интервал
    tags=["spark", "realtime", "ecommerce"],
) as dag:
    
    run_spark_streaming_job = SparkSubmitOperator(
    task_id='run_spark_streaming_job',
   
    conn_id='spark_default',
    application='/opt/airflow/etl/streaming/ingest_kafka_to_iceberg.py',
    # master='spark://spark-master:7077', 
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0',
    conf={
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    },
    # driver_memory='2g', # Опционально: настроить потребление ресурсов
    # executor_memory='3g',
    # num_executors=2,
    # executor_cores=1,
    )

