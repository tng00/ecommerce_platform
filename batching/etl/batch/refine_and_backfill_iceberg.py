# batching/etl/streaming/refine_and_backfill_iceberg.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, date_format, hash, abs, array,
    to_json, from_json, floor, struct, dayofweek, weekofyear, when,
    transform, row_number, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    TimestampType, BooleanType, LongType, DecimalType, DateType,
    ArrayType, MapType
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# --- Spark Session Configuration ---
# Предполагается, что MinIO доступен по имени сервиса 'minio' в той же сети Docker Compose
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://ecommerce-data/iceberg_warehouse")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET", "s3a://ecommerce-data") # Новый бакет для обычных Parquet файлов

spark = SparkSession.builder \
    .appName("EcommerceDataRefinement") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.catalog.minio_iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_iceberg_catalog.warehouse", ICEBERG_WAREHOUSE_PATH) \
    .getOrCreate()

# --- ClickHouse Settings ---
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")

def write_to_clickhouse(df, table_name, mode="append"):
    """
    Writes DataFrame to ClickHouse.
    mode: "append" (for incremental data).
    Note: Tables must be pre-created in ClickHouse with the correct engine (e.g., ReplacingMergeTree).
    """
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}") \
            .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASSWORD) \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()
        print(f"Data successfully written to ClickHouse table: {table_name} with mode: {mode}")
    except Exception as e:
        print(f"Error writing to ClickHouse table {table_name}: {e}")
        raise

def write_to_s3_parquet(df, path, mode="overwrite", partition_by=None):
    """
    Writes DataFrame to S3 as Parquet files.
    """
    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
        print(f"Data successfully written to S3 Parquet at: {path} with mode: {mode}")
    except Exception as e:
        print(f"Error writing to S3 Parquet at {path}: {e}")
        raise

def write_to_iceberg(df, table_name, mode="append", partition_by=None):
    """
    Writes DataFrame to an Iceberg table in S3.
    """
    try:
        writer = df.writeTo(f"minio_iceberg_catalog.ecommerce_db.{table_name}") \
                   .using("iceberg") \
                   .tableProperty("write.format.default", "parquet")

        if mode == "append":
            writer.append()
        elif mode == "overwrite":
            writer.overwritePartitions() # Overwrites based on partition columns if any
        elif mode == "createOrReplace":
            writer.createOrReplace()
        else:
            raise ValueError(f"Unsupported Iceberg write mode: {mode}")

        print(f"Data successfully written to Iceberg table: {table_name} with mode: {mode}")
    except Exception as e:
        print(f"Error writing to Iceberg table {table_name}: {e}")
        raise

def generate_date_dimension(min_date, max_date, load_ts):
    """
    Generates a DataFrame for the date dimension based on a date range.
    """
    date_range = []
    if min_date and max_date:
        current = min_date
        while current <= max_date:
            date_range.append(current)
            current += timedelta(days=1)

    date_data = [(d.year, d.month, d.day, d) for d in date_range]
    date_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("full_date_col", DateType(), True)
    ])
    return spark.createDataFrame(date_data, schema=date_schema)

def process_dimensions():
    """
    Processes and loads dimension tables into ClickHouse and S3 (Iceberg/Parquet).
    """
    print("Processing dimension tables...")
    load_ts = current_timestamp()

    # --- Date Dimensions ---
    orders_iceberg_df_for_dates = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.orders")
    min_date_val = orders_iceberg_df_for_dates.selectExpr("min(order_date)").collect()[0][0]
    max_date_val = orders_iceberg_df_for_dates.selectExpr("max(order_date)").collect()[0][0]

    # Convert to datetime objects if they are strings
    start_date = None
    end_date = None
    if min_date_val:
        start_date = (datetime.strptime(min_date_val, "%Y-%m-%dT%H:%M:%S.%f") if isinstance(min_date_val, str) and '.' in min_date_val else
                      datetime.strptime(min_date_val, "%Y-%m-%dT%H:%M:%S") if isinstance(min_date_val, str) else min_date_val)
    if max_date_val:
        end_date = (datetime.strptime(max_date_val, "%Y-%m-%dT%H:%M:%S.%f") if isinstance(max_date_val, str) and '.' in max_date_val else
                    datetime.strptime(max_date_val, "%Y-%m-%dT%H:%M:%S") if isinstance(max_date_val, str) else max_date_val)

    all_dates_df = generate_date_dimension(start_date, end_date, load_ts)

    # d_years
    d_years_df = all_dates_df.select(
        col("year").alias("year_key").cast(IntegerType()),
        load_ts.alias("load_ts")
    ).distinct()
    write_to_clickhouse(d_years_df, "d_years", mode="overwrite") # Overwrite for full dimension reload
    write_to_iceberg(d_years_df, "d_years", mode="createOrReplace", partition_by=["year_key"]) # Iceberg table
    write_to_s3_parquet(d_years_df, f"{S3_OUTPUT_BUCKET}/dimensions_parquet/d_years/", mode="overwrite", partition_by=["year_key"]) # Raw Parquet
    print("d_years processing complete.")

    # d_months
    d_months_df = all_dates_df.select(
        (col("year") * 100 + col("month")).alias("month_key").cast(IntegerType()),
        col("year").alias("year_key").cast(IntegerType()),
        col("month").alias("month_of_year").cast(IntegerType()),
        date_format(col("full_date_col"), "MMMM").alias("month_name").cast(StringType()),
        (floor((col("month") - 1) / 3) + 1).alias("quarter_of_year").cast(IntegerType()),
        load_ts.alias("load_ts")
    ).distinct()
    write_to_clickhouse(d_months_df, "d_months", mode="overwrite")
    write_to_iceberg(d_months_df, "d_months", mode="createOrReplace", partition_by=["year_key", "month_of_year"])
    print("d_months processing complete.")

    # d_date
    d_date_df = all_dates_df.select(
        (col("year") * 10000 + col("month") * 100 + col("day")).alias("date_key").cast(IntegerType()),
        (col("year") * 100 + col("month")).alias("month_key").cast(IntegerType()),
        col("full_date_col").alias("full_date").cast(DateType()),
        col("day").alias("day_of_month").cast(IntegerType()),
        (((dayofweek(col("full_date_col")) + 5) % 7) + 1).alias("day_of_week").cast(IntegerType()),
        date_format(col("full_date_col"), "EEEE").alias("day_name").cast(StringType()),
        weekofyear(col("full_date_col")).alias("week_of_year").cast(IntegerType()),
        (dayofweek(col("full_date_col")).isin([1, 7])).cast(IntegerType()).alias("is_weekend"),
        load_ts.alias("load_ts")
    ).distinct()
    write_to_clickhouse(d_date_df, "d_date", mode="overwrite")
    write_to_iceberg(d_date_df, "d_date", mode="createOrReplace", partition_by=["year", "month", "day"])
    print("d_date processing complete.")

    # --- Geo Dimensions (Countries, Regions, Cities, Postal Codes, Addresses) ---
    addresses_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.addresses")

    # d_countries
    countries_df = addresses_iceberg_df.select(
        col("country_code").alias("country_code_source").cast(StringType()),
        lit("Unknown").alias("country_name").cast(StringType()), # Placeholder, would ideally join with a lookup table
        load_ts.alias("load_ts")
    ).distinct().withColumn(
        "country_key", abs(hash(col("country_code_source"))).cast(LongType())
    ).withColumn("country_key", when(col("country_code_source").isNull(), lit(0)).otherwise(col("country_key"))) # Handle null hash
    write_to_clickhouse(countries_df, "d_countries", mode="overwrite")
    write_to_iceberg(countries_df, "d_countries", mode="createOrReplace", partition_by=["country_code_source"])
    print("d_countries processing complete.")

    # d_regions
    regions_df = addresses_iceberg_df.alias("a").select(
        col("state_province").alias("region_name_source").cast(StringType()),
        col("country_code").alias("country_code_source").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct().join(countries_df, ["country_code_source"], "left").select(
        col("region_name_source"),
        col("country_key"),
        load_ts.alias("load_ts")
    ).withColumn(
        "region_key", abs(hash(col("region_name_source")) + hash(col("country_key"))).cast(LongType())
    ).withColumn("region_key", when(col("region_name_source").isNull() | col("country_key").isNull(), lit(0)).otherwise(col("region_key")))
    write_to_clickhouse(regions_df, "d_regions", mode="overwrite")
    write_to_iceberg(regions_df, "d_regions", mode="createOrReplace", partition_by=["country_key"])
    print("d_regions processing complete.")

    # d_cities
    cities_df = addresses_iceberg_df.alias("a").select(
        col("city").alias("city_name_source").cast(StringType()),
        col("state_province").alias("region_name_source").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct().join(regions_df, ["region_name_source"], "left").select(
        col("city_name_source"),
        col("region_key"),
        load_ts.alias("load_ts")
    ).withColumn(
        "city_key", abs(hash(col("city_name_source")) + hash(col("region_key"))).cast(LongType())
    ).withColumn("city_key", when(col("city_name_source").isNull() | col("region_key").isNull(), lit(0)).otherwise(col("city_key")))
    write_to_clickhouse(cities_df, "d_cities", mode="overwrite")
    write_to_iceberg(cities_df, "d_cities", mode="createOrReplace", partition_by=["region_key"])
    print("d_cities processing complete.")

    # d_postal_codes
    postal_codes_df = addresses_iceberg_df.select(
        col("postal_code").alias("postal_code_source").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct().withColumn(
        "postal_code_key", abs(hash(col("postal_code_source"))).cast(LongType())
    ).withColumn("postal_code_key", when(col("postal_code_source").isNull(), lit(0)).otherwise(col("postal_code_key")))
    write_to_clickhouse(postal_codes_df, "d_postal_codes", mode="overwrite")
    write_to_iceberg(postal_codes_df, "d_postal_codes", mode="createOrReplace", partition_by=["postal_code_source"])
    print("d_postal_codes processing complete.")

    # d_addresses
    addresses_df = addresses_iceberg_df.alias("a").join(
        cities_df.alias("c"), col("a.city") == col("c.city_name_source"), "left"
    ).join(
        postal_codes_df.alias("pc"), col("a.postal_code") == col("pc.postal_code_source"), "left"
    ).select(
        col("a.address_id").alias("address_id_source").cast(LongType()),
        col("a.street_address").cast(StringType()),
        col("c.city_key").cast(LongType()),
        col("pc.postal_code_key").cast(LongType()),
        load_ts.alias("load_ts")
    ).withColumn(
        "address_key", abs(hash(col("address_id_source"))).cast(LongType())
    ).withColumn("address_key", when(col("address_id_source").isNull(), lit(0)).otherwise(col("address_key")))
    write_to_clickhouse(addresses_df, "d_addresses", mode="overwrite")
    write_to_iceberg(addresses_df, "d_addresses", mode="createOrReplace", partition_by=["address_id_source"])
    print("d_addresses processing complete.")

    # --- Customer Dimension ---
    users_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.users")
    window_spec = Window.partitionBy("user_id").orderBy(col("kafka_timestamp").desc())
    deduplicated_users = users_df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

    customers_df = deduplicated_users.select(
        col("user_id").alias("customer_id_source").cast(LongType()),
        col("username").cast(StringType()),
        col("email").cast(StringType()),
        col("first_name").cast(StringType()),
        col("last_name").cast(StringType()),
        col("date_of_birth").cast(DateType()),
        lit(None).alias("default_shipping_address_key").cast(LongType()), # Populate if available
        lit(None).alias("default_billing_address_key").cast(LongType()),  # Populate if available
        to_date(col("created_at")).alias("registration_date").cast(DateType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    ).withColumn(
        "customer_key", abs(hash(col("customer_id_source"))).cast(LongType())
    ).withColumn("customer_key", when(col("customer_id_source").isNull(), lit(0)).otherwise(col("customer_key")))
    write_to_clickhouse(customers_df, "d_customers", mode="overwrite")
    write_to_iceberg(customers_df, "d_customers", mode="createOrReplace", partition_by=["registration_date"])
    print("d_customers processing complete.")

    # --- Seller Dimension ---
    sellers_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.sellers")
    sellers_df = sellers_iceberg_df.select(
        col("seller_id").alias("seller_id_source").cast(LongType()),
        col("company_name").cast(StringType()),
        to_date(col("registration_date")).alias("registration_date").cast(DateType()),
        col("is_active").alias("is_active").cast(BooleanType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    ).withColumn(
        "seller_key", abs(hash(col("seller_id_source"))).cast(LongType())
    ).withColumn("seller_key", when(col("seller_id_source").isNull(), lit(0)).otherwise(col("seller_key")))
    write_to_clickhouse(sellers_df, "d_sellers", mode="overwrite")
    write_to_iceberg(sellers_df, "d_sellers", mode="createOrReplace", partition_by=["registration_date"])
    print("d_sellers processing complete.")

    # --- Category Dimension ---
    categories_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.categories")
    categories_df = categories_iceberg_df.select(
        col("category_id").alias("category_id_source").cast(LongType()),
        col("name").alias("category_name").cast(StringType()),
        col("parent_category_id").alias("parent_category_id_source").cast(LongType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    ).withColumn(
        "category_key", abs(hash(col("category_id_source"))).cast(LongType())
    ).withColumn("category_key", when(col("category_id_source").isNull(), lit(0)).otherwise(col("category_key")))

    categories_df = categories_df.alias("c").join(
        categories_df.alias("pc"),
        col("c.parent_category_id_source") == col("pc.category_id_source"),
        "left"
    ).select(
        col("c.category_key"),
        col("c.category_id_source"),
        col("c.category_name"),
        when(col("pc.category_key").isNull(), lit(0)).otherwise(col("pc.category_key")).alias("parent_category_key").cast(LongType()),
        col("c.load_ts"),
        col("c.source_system")
    )
    write_to_clickhouse(categories_df, "d_categories", mode="overwrite")
    write_to_iceberg(categories_df, "d_categories", mode="createOrReplace", partition_by=["category_name"])
    print("d_categories processing complete.")

    # --- Product Dimension ---
    products_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.products")
    product_details_mongo_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.product_details")

    products_df = products_iceberg_df.alias("p").join(
        categories_df.alias("c"), col("p.category_id") == col("c.category_id_source"), "left"
    ).join(
        sellers_df.alias("s"), col("p.seller_id") == col("s.seller_id_source"), "left"
    ).join(
        product_details_mongo_df.alias("pd"), col("p.product_id") == col("pd.product_id_pg"), "left"
    ).select(
        col("p.product_id").alias("product_id_source").cast(LongType()),
        col("p.name").alias("product_name").cast(StringType()),
        col("p.description").cast(StringType()),
        col("p.sku").cast(StringType()),
        when(col("c.category_key").isNull(), lit(0)).otherwise(col("c.category_key")).alias("category_key").cast(LongType()),
        when(col("s.seller_key").isNull(), lit(0)).otherwise(col("s.seller_key")).alias("seller_key").cast(LongType()),
        to_json(col("pd.tags")).cast(StringType()).alias("tags"),
        to_json(col("pd.attributes")).cast(StringType()).alias("mongo_attributes"),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    ).withColumn(
        "product_key", abs(hash(col("product_id_source"))).cast(LongType())
    ).withColumn("product_key", when(col("product_id_source").isNull(), lit(0)).otherwise(col("product_key")))
    write_to_clickhouse(products_df, "d_products", mode="overwrite")
    write_to_iceberg(products_df, "d_products", mode="createOrReplace", partition_by=["category_key", "seller_key"])
    print("d_products processing complete.")

    # --- Review Dimension ---
    reviews_mongo_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.reviews")
    reviews_df = reviews_mongo_df.alias("r").join(
        products_df.alias("p"), col("r.product_id_pg") == col("p.product_id_source"), "left"
    ).join(
        customers_df.alias("c"), col("r.user_id_pg") == col("c.customer_id_source"), "left"
    ).select(
        col("r._id").alias("review_id_source").cast(StringType()),
        when(col("p.product_key").isNull(), lit(0)).otherwise(col("p.product_key")).alias("product_key").cast(LongType()),
        when(col("c.customer_key").isNull(), lit(0)).otherwise(col("c.customer_key")).alias("customer_key").cast(LongType()),
        col("r.rating").cast(IntegerType()),
        col("r.title").cast(StringType()),
        col("r.comment").cast(StringType()),
        lit(None).cast(StringType()).alias("image_urls"), # Assuming no image_urls in source or need transformation
        col("r.is_verified_purchase").cast(IntegerType()).alias("is_verified_purchase"),
        col("r.review_date").cast(TimestampType()).alias("review_created_at_source"),
        col("r.updated_at").cast(TimestampType()).alias("review_updated_at_source"),
        load_ts.alias("load_ts"),
        lit("MongoDB").alias("source_system")
    ).withColumn(
        "review_key", abs(hash(col("review_id_source"))).cast(LongType())
    ).withColumn("review_key", when(col("review_id_source").isNull(), lit(0)).otherwise(col("review_key")))
    write_to_clickhouse(reviews_df, "d_reviews", mode="overwrite")
    write_to_iceberg(reviews_df, "d_reviews", mode="createOrReplace", partition_by=[to_date("review_created_at_source")])
    print("d_reviews processing complete.")

    # --- Order Dimension ---
    orders_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.orders")
    shipments_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.shipment_status_updates")
    payments_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.payments")

    # Deduplicate shipments and payments to get latest status
    window_spec_shipment = Window.partitionBy("order_id").orderBy(col("kafka_timestamp").desc())
    latest_shipments_df = shipments_df.withColumn("rn", row_number().over(window_spec_shipment)).filter("rn = 1").drop("rn")

    window_spec_payment = Window.partitionBy("order_id").orderBy(col("kafka_timestamp").desc())
    latest_payments_df = payments_df.withColumn("rn", row_number().over(window_spec_payment)).filter("rn = 1").drop("rn")

    # Determine payment method based on 'gateway_data' column presence
    payment_method_expression = lit(None).cast(StringType())
    if "gateway_data" in latest_payments_df.columns:
        payment_method_expression = when(col("pay.gateway_data").isNotNull(),
                                         from_json(col("pay.gateway_data"), MapType(StringType(), StringType()))["payment_processor"]) \
                                   .otherwise(lit(None)).cast(StringType())
    else:
        print("Warning: 'gateway_data' column not found in 'payments' table. 'payment_method' will be NULL.")

    orders_dim_df = orders_iceberg_df.alias("o").join(
        addresses_df.alias("sa"), col("o.shipping_address_id") == col("sa.address_id_source"), "left"
    ).join(
        addresses_df.alias("ba"), col("o.billing_address_id") == col("ba.address_id_source"), "left"
    ).join(
        customers_df.alias("cust"), col("o.user_id") == col("cust.customer_id_source"), "left"
    ).join(
        latest_shipments_df.alias("ship"), col("o.order_id") == col("ship.order_id"), "left"
    ).join(
        latest_payments_df.alias("pay"), col("o.order_id") == col("pay.order_id"), "left"
    ).select(
        col("o.order_id").alias("order_id_source").cast(LongType()),
        when(col("cust.customer_key").isNull(), lit(0)).otherwise(col("cust.customer_key")).alias("customer_key").cast(LongType()),
        date_format(to_date(col("o.order_date")), "yyyyMMdd").alias("order_date_key").cast(IntegerType()),
        col("o.status").alias("current_order_status").cast(StringType()),
        col("o.updated_at").alias("current_order_status_updated_at").cast(TimestampType()),
        when(col("sa.address_key").isNull(), lit(0)).otherwise(col("sa.address_key")).alias("shipping_address_key").cast(LongType()),
        when(col("ba.address_key").isNull(), lit(0)).otherwise(col("ba.address_key")).alias("billing_address_key").cast(LongType()),
        col("o.shipping_method").alias("shipping_method").cast(StringType()),
        when(col("ship.tracking_number").isNotNull(), col("ship.tracking_number"))
        .otherwise(col("o.tracking_number")).alias("current_tracking_number").cast(StringType()),
        col("ship.location").alias("current_shipment_location").cast(StringType()),
        when(col("pay.status").isNotNull(), col("pay.status"))
        .otherwise(lit("Pending")).alias("current_payment_status").cast(StringType()),
        payment_method_expression.alias("payment_method"),
        col("pay.transaction_id").alias("last_transaction_id").cast(StringType()),
        col("o.total_amount").alias("order_total_amount_source").cast(DecimalType(12, 2)),
        col("o.created_at").alias("order_created_at_source").cast(TimestampType()),
        col("o.updated_at").alias("order_updated_at_source").cast(TimestampType()),
        col("o.notes").cast(StringType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    ).withColumn(
        "order_key", abs(hash(col("order_id_source"))).cast(LongType())
    ).withColumn("order_key", when(col("order_id_source").isNull(), lit(0)).otherwise(col("order_key")))
    write_to_clickhouse(orders_dim_df, "d_orders", mode="overwrite")
    write_to_iceberg(orders_dim_df, "d_orders", mode="createOrReplace", partition_by=["order_date_key"])
    print("d_orders processing complete.")

    print("Dimension tables processing complete.")


def process_fact_table():
    """
    Processes and loads the f_sales fact table into ClickHouse and S3 (Iceberg/Parquet).
    Fact tables are typically append-only for new data, but for backfill and initial load, 'overwrite' or 'createOrReplace' might be used.
    """
    print("Processing fact table f_sales...")
    load_ts = current_timestamp()

    # Load necessary source tables and previously processed dimensions
    orders_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.orders")
    order_items_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.order_items")
    products_df_for_fact = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.products")
    sellers_df_for_fact = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.sellers")

    # Load dimension tables to get their keys
    customers_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.d_customers")
    products_dim_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.d_products")
    sellers_dim_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.d_sellers")
    addresses_dim_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.d_addresses")
    orders_dim_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.d_orders") # To get order_key

    sales_pre_join = orders_df.alias("o").join(
        order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"), "inner"
    ).join(
        products_df_for_fact.alias("p_src"), col("oi.product_id") == col("p_src.product_id"), "left"
    ).join(
        sellers_df_for_fact.alias("s_src"), col("p_src.seller_id") == col("s_src.seller_id"), "left"
    ).select(
        col("oi.order_item_id").alias("order_item_id_source").cast(LongType()),
        col("o.order_id").alias("order_id_source").cast(LongType()),
        to_date(col("o.order_date")).alias("order_date_source").cast(DateType()),
        col("oi.product_id").alias("product_id_source").cast(LongType()),
        col("o.user_id").alias("customer_id_source").cast(LongType()),
        col("s_src.seller_id").alias("seller_id_source").cast(LongType()),
        col("o.shipping_address_id").alias("shipping_address_id_source").cast(LongType()),
        col("o.billing_address_id").alias("billing_address_id_source").cast(LongType()),
        col("oi.quantity").alias("quantity_sold").cast(IntegerType()),
        col("oi.unit_price").alias("unit_price_at_sale").cast(DecimalType(10, 2)),
        (col("oi.quantity") * col("oi.unit_price")).alias("total_amount_item").cast(DecimalType(12, 2)),
        col("o.created_at").alias("event_ts").cast(TimestampType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    )

    # Join with dimension tables to get surrogate keys
    f_sales_df = sales_pre_join.alias("f").join(
        products_dim_df.alias("dp"), col("f.product_id_source") == col("dp.product_id_source"), "left"
    ).join(
        customers_df.alias("dc"), col("f.customer_id_source") == col("dc.customer_id_source"), "left"
    ).join(
        sellers_dim_df.alias("ds"), col("f.seller_id_source") == col("ds.seller_id_source"), "left"
    ).join(
        orders_dim_df.alias("do"), col("f.order_id_source") == col("do.order_id_source"), "left"
    ).join(
        addresses_dim_df.alias("dsa"), col("f.shipping_address_id_source") == col("dsa.address_id_source"), "left"
    ).join(
        addresses_dim_df.alias("dba"), col("f.billing_address_id_source") == col("dba.address_id_source"), "left"
    ).select(
        col("f.order_item_id_source"),
        col("f.order_id_source"),
        col("do.order_key").alias("order_key").cast(LongType()), # Use order_key from d_orders
        date_format(col("f.order_date_source"), "yyyyMMdd").alias("order_date_key").cast(IntegerType()),
        when(col("dp.product_key").isNull(), lit(0)).otherwise(col("dp.product_key")).alias("product_key").cast(LongType()),
        when(col("dc.customer_key").isNull(), lit(0)).otherwise(col("dc.customer_key")).alias("customer_key").cast(LongType()),
        when(col("ds.seller_key").isNull(), lit(0)).otherwise(col("ds.seller_key")).alias("seller_key").cast(LongType()),
        when(col("dsa.address_key").isNull(), lit(0)).otherwise(col("dsa.address_key")).alias("shipping_address_key").cast(LongType()),
        when(col("dba.address_key").isNull(), lit(0)).otherwise(col("dba.address_key")).alias("billing_address_key").cast(LongType()),
        col("f.quantity_sold"),
        col("f.unit_price_at_sale"),
        col("f.total_amount_item"),
        col("f.event_ts"),
        col("f.load_ts"),
        col("f.source_system")
    )

    # Ensure all key columns are non-null by replacing nulls with 0
    f_sales_df = f_sales_df.withColumn("order_key", when(col("order_key").isNull(), lit(0)).otherwise(col("order_key"))) \
                           .withColumn("product_key", when(col("product_key").isNull(), lit(0)).otherwise(col("product_key"))) \
                           .withColumn("customer_key", when(col("customer_key").isNull(), lit(0)).otherwise(col("customer_key"))) \
                           .withColumn("seller_key", when(col("seller_key").isNull(), lit(0)).otherwise(col("seller_key"))) \
                           .withColumn("shipping_address_key", when(col("shipping_address_key").isNull(), lit(0)).otherwise(col("shipping_address_key"))) \
                           .withColumn("billing_address_key", when(col("billing_address_key").isNull(), lit(0)).otherwise(col("billing_address_key")))

    write_to_clickhouse(f_sales_df, "f_sales", mode="append")
    write_to_iceberg(f_sales_df, "f_sales", mode="append", partition_by=["order_date_key"]) # Append for facts
    write_to_s3_parquet(f_sales_df, f"{S3_OUTPUT_BUCKET}/facts_parquet/f_sales/", mode="append", partition_by=["order_date_key"]) # Append for raw Parquet
    print("Fact table f_sales processing complete.")


if __name__ == "__main__":
    print("Starting Spark batch processing for data refinement and backfill...")
    process_dimensions()
    process_fact_table()
    spark.stop()
    print("Spark batch processing finished.")