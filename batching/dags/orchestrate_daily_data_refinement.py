from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_batch_processing",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule="0 0 * * *",  # Запуск раз в сутки в полночь UTC
    tags=["spark", "batch", "ecommerce"],
) as dag:
    run_daily_batch_job = BashOperator(
        task_id="run_spark_daily_batch_job",
        bash_command="""
            set -e
            /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minioadmin \
                --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \ # Оставляем пакеты, которые не являются прямыми JAR-файлами
                /opt/airflow/etl/batch/refine_and_backfill_iceberg.py 
        """,
    )
