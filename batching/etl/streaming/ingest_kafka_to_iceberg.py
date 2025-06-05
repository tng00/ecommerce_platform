# batching/etl/streaming/ingest_kafka_to_iceberg.py
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType, MapType, ArrayType

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

# --- Spark Session Configuration ---
# Получаем настройки из переменных окружения
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000") # Теперь это значение будет использоваться
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://ecommerce-data/iceberg_warehouse")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092") # Добавим и для Kafka

spark = SparkSession.builder \
    .appName("KafkaToIcebergPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "360s") \
    .config("spark.python.worker.timeout", "120") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", ICEBERG_WAREHOUSE_PATH) \
    .getOrCreate()

print("Spark Session initialized and MinIO connection configured.")

try:
    print("Running basic test...")
    data = [("Test", 1), ("Connection", 2)]
    df = spark.createDataFrame(data, ["Word", "Count"])
    df.show()
    print("Basic test passed!")
except Exception as e:
    print(f"Basic test failed! Error: {e}")


def get_schema(topic):
    if "pg_ecomm_cdc" in topic:
        if "users" in topic:
            return StructType([
                StructField("user_id", IntegerType(), True),
                StructField("username", StringType(), True),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("date_of_birth", StringType(), True),
                StructField("phone_number", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "products" in topic:
            return StructType([
                StructField("product_id", IntegerType(), True),
                StructField("seller_id", IntegerType(), True),
                StructField("category_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("base_price", FloatType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "inventory_pg" in topic:
            return StructType([
                StructField("inventory_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("reserved_quantity", IntegerType(), True),
                StructField("last_restock_date", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "addresses" in topic:
            return StructType([
                StructField("address_id", IntegerType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("street_address", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("address_type", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "sellers" in topic:
            return StructType([
                StructField("seller_id", IntegerType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("company_name", StringType(), True),
                StructField("tax_id", StringType(), True),
                StructField("registration_date", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "categories" in topic:
            return StructType([
                StructField("category_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("parent_category_id", IntegerType(), True),
                StructField("description", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "orders" in topic:
            return StructType([
                StructField("order_id", IntegerType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("order_date", StringType(), True),
                StructField("status", StringType(), True),
                StructField("total_amount", FloatType(), True),
                StructField("shipping_address_id", IntegerType(), True),
                StructField("billing_address_id", IntegerType(), True),
                StructField("shipping_method", StringType(), True),
                StructField("tracking_number", StringType(), True),
                StructField("notes", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "order_items" in topic:
            return StructType([
                StructField("order_item_id", IntegerType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", FloatType(), True),
                StructField("subtotal", FloatType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "payments" in topic:
            return StructType([
                StructField("payment_id", IntegerType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("payment_date", StringType(), True),
                StructField("payment_method", StringType(), True),
                StructField("transaction_id", StringType(), True),
                StructField("amount", FloatType(), True),
                StructField("status", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True)
            ])

    elif "mongo_ecomm_cdc" in topic:
        if "product_details_mongo" in topic:
            return StructType([
                StructField("_id", StringType(), True),
                StructField("product_id_pg", IntegerType(), True),
                StructField("tags", ArrayType(StringType()), True),
                StructField("images", ArrayType(MapType(StringType(), StringType())), True),
                StructField("attributes", MapType(StringType(), StringType()), True),
                StructField("manufacturer_info", MapType(StringType(), StringType()), True),
                StructField("dimensions", MapType(StringType(), StringType()), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "user_profiles_mongo" in topic:
            return StructType([
                StructField("_id", StringType(), True),
                StructField("user_id_pg", IntegerType(), True),
                StructField("preferences", MapType(StringType(), StringType()), True),
                StructField("Browse_history", ArrayType(MapType(StringType(), StringType())), True),
                StructField("wishlist", ArrayType(MapType(StringType(), StringType())), True),
                StructField("last_login_ip", StringType(), True),
                StructField("loyalty_points", IntegerType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "reviews_mongo" in topic:
            return StructType([
                StructField("_id", StringType(), True),
                StructField("product_id_pg", IntegerType(), True),
                StructField("user_id_pg", IntegerType(), True),
                StructField("rating", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("comment", StringType(), True),
                StructField("review_date", StringType(), True),
                StructField("is_verified_purchase", BooleanType(), True),
                StructField("helpful_votes", IntegerType(), True),
                StructField("updated_at", StringType(), True)
            ])
        elif "seller_profiles_mongo" in topic:
            return StructType([
                StructField("_id", StringType(), True),
                StructField("seller_id_pg", IntegerType(), True),
                StructField("profile_description", StringType(), True),
                StructField("logo_url", StringType(), True),
                StructField("banner_url", StringType(), True),
                StructField("social_media_links", MapType(StringType(), StringType()), True),
                StructField("return_policy", StringType(), True),
                StructField("customer_service_contact", MapType(StringType(), StringType()), True),
                StructField("average_rating", FloatType(), True),
                StructField("updated_at", StringType(), True)
            ])

    elif topic in ["inventory_adjustments", "payment_gateway_callbacks", "shipment_status_updates"]:
        if topic == "inventory_adjustments":

            return StructType([
                StructField("product_id", IntegerType(), True),
                StructField("quantity_change", IntegerType(), True),
                StructField("reason", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])
        elif topic == "payment_gateway_callbacks":
            return StructType([
                StructField("order_id", IntegerType(), True),
                StructField("transaction_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("gateway_data", MapType(StringType(), StringType()), True),
                StructField("timestamp", StringType(), True)
            ])
        elif topic == "shipment_status_updates":
            return StructType([
                StructField("order_id", IntegerType(), True),
                StructField("tracking_number", StringType(), True),
                StructField("new_status", StringType(), True),
                StructField("location", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])

    return None 


def process_kafka_topic(topic):
    print(f"Starting to process topic: {topic}")

    json_schema = get_schema(topic) 
    if json_schema is None:
        print(f"ERROR: No explicit schema defined for topic: {topic}. Skipping.")
        return None

 
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()


    parsed_df_base = kafka_df.select(
        from_json(col("value").cast("string"), json_schema).alias("data_payload"),
        col("timestamp").alias("kafka_timestamp") 
    )


    select_cols_from_payload = [col(f"data_payload.{field.name}") for field in json_schema.fields]

    
    if topic == "inventory_adjustments":
       
        if any(field.name == "timestamp" for field in json_schema.fields):

            select_cols_from_payload = [
                col(f"data_payload.{field.name}")
                for field in json_schema.fields
                if field.name != "timestamp"
            ]

    
    parsed_df = parsed_df_base.select(select_cols_from_payload + [col("kafka_timestamp")])

    
    for field_name in parsed_df.columns:
       
        if field_name != "kafka_timestamp" and \
           isinstance(parsed_df.schema[field_name].dataType, StringType) and \
           ("_at" in field_name or "date" in field_name or "timestamp" in field_name):
            parsed_df = parsed_df.withColumn(field_name, to_timestamp(col(field_name)))


    # --- Define the table name for Iceberg ---
   
    table_name = ""

    if "pg_ecomm_cdc.public." in topic:
        table_name = "iceberg_catalog.ecommerce_db." + topic.replace("pg_ecomm_cdc.public.", "")
    elif "mongo_ecomm_cdc.ecommerce_db." in topic: 
        table_name = "iceberg_catalog.ecommerce_db." + topic.replace("mongo_ecomm_cdc.ecommerce_db.", "").replace("_mongo", "")
    else:
        
       
        table_name = f"iceberg_catalog.ecommerce_db.{topic.replace('_mongo', '')}" 

    print(f"Writing to Iceberg table: {table_name}")

    # Write to S3 in Iceberg format
    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", f"s3a://ecommerce-data/checkpoints/{topic}") \
        .toTable(table_name)

    return query

# List of topics to process
topics = [
    
    "pg_ecomm_cdc.public.users",
    "pg_ecomm_cdc.public.addresses",
    "pg_ecomm_cdc.public.sellers",
    "pg_ecomm_cdc.public.categories",
    "pg_ecomm_cdc.public.products",
    "pg_ecomm_cdc.public.inventory_pg",
    "pg_ecomm_cdc.public.orders",
    "pg_ecomm_cdc.public.order_items",
    "pg_ecomm_cdc.public.payments",
    
    "mongo_ecomm_cdc.ecommerce_db.product_details_mongo",
    "mongo_ecomm_cdc.ecommerce_db.user_profiles_mongo",
    "mongo_ecomm_cdc.ecommerce_db.reviews_mongo",
    "mongo_ecomm_cdc.ecommerce_db.seller_profiles_mongo",
    "inventory_adjustments",
    "payment_gateway_callbacks",
    "shipment_status_updates"
]


queries = []
for topic in topics:
    query = process_kafka_topic(topic)
    if query:
        queries.append(query)

print("All streaming queries started. Awaiting termination...")


if queries:
    spark.streams.awaitAnyTermination()
else:
    print("No streaming queries were started.")

print("All Kafka to Iceberg pipelines terminated.")