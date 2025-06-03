# batching/etl/streaming/refine_and_backfill_iceberg.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, date_format, hash, abs, array, to_json, from_json, floor, struct, dayofweek, weekofyear, when, transform
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType, LongType, DecimalType, DateType, ArrayType, MapType

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, to_date


spark = SparkSession.builder \
    .appName("ClickHouseStarSchemaBatchLoad") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.catalog.minio_iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_iceberg_catalog.warehouse", "s3a://ecommerce-data/iceberg_warehouse") \
    .getOrCreate()

# ClickHouse Settings 
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""
CLICKHOUSE_DB = "default"

def write_to_clickhouse(df, table_name, mode="append"):
    """
    Writes DataFrame to ClickHouse.
    mode: "append" (for incremental data).
    Note: Tables must be pre-created in ClickHouse with the correct engine (e.g., ReplacingMergeTree).
    """
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("dbtable", table_name) \
        .mode(mode) \
        .save()
    print(f"Data written to ClickHouse table: {table_name} with mode: {mode}")

def process_dimensions():
    """
    Processes and loads dimension tables.
    """
    print("Processing dimension tables...")
    load_ts = current_timestamp()

   
    orders_iceberg_df_for_dates = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.orders")
    min_date_row = orders_iceberg_df_for_dates.selectExpr("min(order_date)").collect()
    max_date_row = orders_iceberg_df_for_dates.selectExpr("max(order_date)").collect()

    min_date = min_date_row[0][0] if min_date_row and min_date_row[0][0] else None
    max_date = max_date_row[0][0] if max_date_row and max_date_row[0][0] else None

    from datetime import datetime, timedelta
    
    start_date = None
    end_date = None

    if min_date:
        if isinstance(min_date, str):
            try:
                start_date = datetime.strptime(min_date, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                start_date = datetime.strptime(min_date, "%Y-%m-%dT%H:%M:%S")
        else:
            start_date = min_date
    
    if max_date:
        if isinstance(max_date, str):
            try:
                end_date = datetime.strptime(max_date, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                end_date = datetime.strptime(max_date, "%Y-%m-%dT%H:%M:%S")
        else:
            end_date = max_date

    date_range = []
    if start_date and end_date:
        current = start_date
        while current <= end_date:
            date_range.append(current)
            current += timedelta(days=1)
    
    date_data = [(d.year, d.month, d.day, d) for d in date_range]
    date_schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("full_date_col", DateType(), True)
    ])
    all_dates_df = spark.createDataFrame(date_data, schema=date_schema)

    # All dimension DataFrames include load_ts explicitly for ReplacingMergeTree
    
    
    d_years_df = all_dates_df.select(
        col("year").alias("year_key").cast(IntegerType()),
        load_ts.alias("load_ts")
    ).distinct()
    write_to_clickhouse(d_years_df, "d_years")
    print("d_years processing complete.")

    
    d_months_df = all_dates_df.select(
        (col("year") * 100 + col("month")).alias("month_key").cast(IntegerType()),
        col("year").alias("year_key").cast(IntegerType()),
        col("month").alias("month_of_year").cast(IntegerType()),
        date_format(col("full_date_col"), "MMMM").alias("month_name").cast(StringType()),
        (floor((col("month") - 1) / 3) + 1).alias("quarter_of_year").cast(IntegerType()),
        load_ts.alias("load_ts")
    ).distinct()
    write_to_clickhouse(d_months_df, "d_months")
    print("d_months processing complete.")


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
    write_to_clickhouse(d_date_df, "d_date")
    print("d_date processing complete.")

    
    addresses_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.addresses")
    countries_df = addresses_iceberg_df.select(
        col("country_code").alias("country_code_source").cast(StringType()),
        lit("Unknown").alias("country_name").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct()
    countries_df = countries_df.withColumn("country_key", abs(hash(col("country_code_source"))).cast(LongType()))
    write_to_clickhouse(countries_df, "d_countries")
    print("d_countries processing complete.")

    
    regions_df = addresses_iceberg_df.alias("a").select(
        col("state_province").alias("region_name_source").cast(StringType()),
        col("country_code").alias("country_code_source").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct().join(countries_df, ["country_code_source"], "left").select(
        col("region_name_source"),
        col("country_key"),
        load_ts.alias("load_ts")
    )
    regions_df = regions_df.withColumn("region_key", abs(hash(col("region_name_source")) + hash(col("country_key"))).cast(LongType()))
    write_to_clickhouse(regions_df, "d_regions")
    print("d_regions processing complete.")

   
    cities_df = addresses_iceberg_df.alias("a").select(
        col("city").alias("city_name_source").cast(StringType()),
        col("state_province").alias("region_name_source").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct().join(regions_df, ["region_name_source"], "left").select(
        col("city_name_source"),
        col("region_key"),
        load_ts.alias("load_ts")
    )
    cities_df = cities_df.withColumn("city_key", abs(hash(col("city_name_source")) + hash(col("region_key"))).cast(LongType()))
    write_to_clickhouse(cities_df, "d_cities")
    print("d_cities processing complete.")

    
    postal_codes_df = addresses_iceberg_df.select(
        col("postal_code").alias("postal_code_source").cast(StringType()),
        load_ts.alias("load_ts")
    ).distinct()
    postal_codes_df = postal_codes_df.withColumn("postal_code_key", abs(hash(col("postal_code_source"))).cast(LongType()))
    write_to_clickhouse(postal_codes_df, "d_postal_codes")
    print("d_postal_codes processing complete.")

    
    addresses_df = addresses_iceberg_df.alias("a").join(
        cities_df.alias("c"),
        col("a.city") == col("c.city_name_source"),
        "left"
    ).join(
        postal_codes_df.alias("pc"),
        col("a.postal_code") == col("pc.postal_code_source"),
        "left"
    ).select(
        col("a.address_id").alias("address_id_source").cast(LongType()),
        col("a.street_address").cast(StringType()),
        col("c.city_key").cast(LongType()),
        col("pc.postal_code_key").cast(LongType()),
        load_ts.alias("load_ts")
    )
    addresses_df = addresses_df.withColumn("address_key", abs(hash(col("address_id_source"))).cast(LongType()))
    write_to_clickhouse(addresses_df, "d_addresses")
    print("d_addresses processing complete.")

    
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
        lit(None).alias("default_shipping_address_key").cast(LongType()),
        lit(None).alias("default_billing_address_key").cast(LongType()),
        to_date(col("created_at")).alias("registration_date").cast(DateType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    )
    customers_df = customers_df.withColumn("customer_key", abs(hash(col("customer_id_source"))).cast(LongType()))
    write_to_clickhouse(customers_df, "d_customers")
    print("d_customers processing complete.")

    
    sellers_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.sellers")
    
   

    sellers_df = sellers_iceberg_df.select(
        col("seller_id").alias("seller_id_source").cast(LongType()),
        col("company_name").cast(StringType()),

        to_date(col("registration_date")).alias("registration_date").cast(DateType()),
        col("is_active").alias("is_active").cast(BooleanType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    )
    sellers_df = sellers_df.withColumn("seller_key", abs(hash(col("seller_id_source"))).cast(LongType()))
    write_to_clickhouse(sellers_df, "d_sellers")
    print("d_sellers processing complete.")

    
    categories_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.categories")
    categories_df = categories_iceberg_df.select(
        col("category_id").alias("category_id_source").cast(LongType()),
        col("name").alias("category_name").cast(StringType()),
        col("parent_category_id").alias("parent_category_id_source").cast(LongType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    )
    categories_df = categories_df.withColumn("category_key", abs(hash(col("category_id_source"))).cast(LongType()))

    categories_df = categories_df.alias("c").join(
        categories_df.alias("pc"),
        col("c.parent_category_id_source") == col("pc.category_id_source"),
        "left"
    ).select(
        col("c.category_key"),
        col("c.category_id_source"),
        col("c.category_name"),
        col("pc.category_key").alias("parent_category_key").cast(LongType()),
        col("c.load_ts"),
        col("c.source_system")
    )
    write_to_clickhouse(categories_df, "d_categories")
    print("d_categories processing complete.")

    
    products_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.products")
    product_details_mongo_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.product_details")

    products_df = products_iceberg_df.alias("p").join(
        categories_df.alias("c"),
        col("p.category_id") == col("c.category_id_source"),
        "left"
    ).join(
        sellers_df.alias("s"),
        col("p.seller_id") == col("s.seller_id_source"),
        "left"
    ).join(
        product_details_mongo_df.alias("pd"),
        col("p.product_id") == col("pd.product_id_pg"),
        "left"
    ).select(
        col("p.product_id").alias("product_id_source").cast(LongType()),
        col("p.name").alias("product_name").cast(StringType()),
        col("p.description").cast(StringType()),
        col("p.sku").cast(StringType()),
        col("c.category_key").cast(LongType()),
        col("s.seller_key").cast(LongType()),
        to_json(col("pd.tags")).cast(StringType()).alias("tags"),
        to_json(col("pd.attributes")).cast(StringType()).alias("mongo_attributes"),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    )
    products_df = products_df.withColumn("product_key", abs(hash(col("product_id_source"))).cast(LongType()))
    write_to_clickhouse(products_df, "d_products")
    print("d_products processing complete.")

   
    reviews_mongo_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.reviews")
    reviews_df = reviews_mongo_df.alias("r").join(
        products_df.alias("p"),
        col("r.product_id_pg") == col("p.product_id_source"),
        "left"
    ).join(
        customers_df.alias("c"),
        col("r.user_id_pg") == col("c.customer_id_source"),
        "left"
    ).select(
        col("r._id").alias("review_id_source").cast(StringType()),
        col("p.product_key").cast(LongType()),
        col("c.customer_key").cast(LongType()),
        col("r.rating").cast(IntegerType()),
        col("r.title").cast(StringType()),
        col("r.comment").cast(StringType()),
        lit(None).cast(StringType()).alias("image_urls"),
        col("r.is_verified_purchase").cast(IntegerType()).alias("is_verified_purchase"),
        col("r.review_date").cast(TimestampType()).alias("review_created_at_source"),
        col("r.updated_at").cast(TimestampType()).alias("review_updated_at_source"),
        load_ts.alias("load_ts"),
        lit("MongoDB").alias("source_system")
    )
    reviews_df = reviews_df.withColumn("review_key", abs(hash(col("review_id_source"))).cast(LongType()))
    write_to_clickhouse(reviews_df, "d_reviews")
    print("d_reviews processing complete.")

    
    orders_iceberg_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.orders")
    
    
    shipments_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.shipment_status_updates")
    
    window_spec_shipment = Window.partitionBy("order_id").orderBy(col("kafka_timestamp").desc())
    latest_shipments_df = shipments_df.withColumn("rn", row_number().over(window_spec_shipment)).filter("rn = 1").drop("rn")

    
    payments_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.payments")
    
    window_spec_payment = Window.partitionBy("order_id").orderBy(col("kafka_timestamp").desc())
    latest_payments_df = payments_df.withColumn("rn", row_number().over(window_spec_payment)).filter("rn = 1").drop("rn")

    
    payment_method_expression = lit(None).cast(StringType()) 
    if "gateway_data" in latest_payments_df.columns:
        payment_method_expression = when(col("pay.gateway_data").isNotNull(), 
                                         from_json(col("pay.gateway_data"), MapType(StringType(), StringType()))["payment_processor"]) \
                                   .otherwise(lit(None)).cast(StringType())
    else:
        print("Warning: 'gateway_data' column not found in 'payments' table. 'payment_method' will be NULL.")


    orders_dim_df = orders_iceberg_df.alias("o").join(
        addresses_df.alias("sa"),
        col("o.shipping_address_id") == col("sa.address_id_source"),
        "left"
    ).join(
        addresses_df.alias("ba"),
        col("o.billing_address_id") == col("ba.address_id_source"), 
        "left"
    ).join(
        customers_df.alias("cust"),
        col("o.user_id") == col("cust.customer_id_source"),
        "left"
    ).join(
        latest_shipments_df.alias("ship"), 
        col("o.order_id") == col("ship.order_id"),
        "left"
    ).join(
        latest_payments_df.alias("pay"), 
        col("o.order_id") == col("pay.order_id"),
        "left"
    ).select(
        col("o.order_id").alias("order_id_source").cast(LongType()),
        col("cust.customer_key").cast(LongType()),
        date_format(to_date(col("o.order_date")), "yyyyMMdd").alias("order_date_key").cast(IntegerType()),
        col("o.status").alias("current_order_status").cast(StringType()),
        col("o.updated_at").alias("current_order_status_updated_at").cast(TimestampType()),
        col("sa.address_key").alias("shipping_address_key").cast(LongType()),
        col("ba.address_key").alias("billing_address_key").cast(LongType()),
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
    )
    orders_dim_df = orders_dim_df.withColumn("order_key", abs(hash(col("order_id_source"))).cast(LongType()))
    write_to_clickhouse(orders_dim_df, "d_orders")
    print("d_orders processing complete.")

    print("Dimension tables processing complete.")

def process_fact_table():
    """
    Processes and loads the f_sales fact table.
    Fact tables are typically append-only for new data.
    """
    print("Processing fact table f_sales...")
    load_ts = current_timestamp()

   
    orders_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.orders")
    order_items_df = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.order_items")
    products_df_for_fact = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.products")
    sellers_df_for_fact = spark.read.format("iceberg").load("minio_iceberg_catalog.ecommerce_db.sellers")

   
    sales_pre_join = orders_df.alias("o").join(
        order_items_df.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    ).join(
        products_df_for_fact.alias("p"), 
        col("oi.product_id") == col("p.product_id"),
        "left"
    ).join(
        sellers_df_for_fact.alias("s"), 
        col("p.seller_id") == col("s.seller_id"),
        "left"
    ).select(
        col("oi.order_item_id").alias("order_item_id_source").cast(LongType()),
        col("o.order_id").alias("order_id_source").cast(LongType()),
        to_date(col("o.order_date")).alias("order_date_source").cast(DateType()),
        col("oi.product_id").alias("product_id_source").cast(LongType()),
        col("o.user_id").alias("customer_id_source").cast(LongType()),
        col("s.seller_id").alias("seller_id_source").cast(LongType()), 
        col("o.shipping_address_id").alias("shipping_address_id_source").cast(LongType()),
        col("o.billing_address_id").alias("billing_address_id_source").cast(LongType()),
        col("oi.quantity").alias("quantity_sold").cast(IntegerType()),
        col("oi.unit_price").alias("unit_price_at_sale").cast(DecimalType(10, 2)),
        (col("oi.quantity") * col("oi.unit_price")).alias("total_amount_item").cast(DecimalType(12, 2)),
        col("o.created_at").alias("event_ts").cast(TimestampType()),
        load_ts.alias("load_ts"),
        lit("Kafka_Streaming_Iceberg").alias("source_system")
    )
    print(sales_pre_join)
    
    f_sales_df = sales_pre_join \
        .withColumn("product_key", abs(hash(col("product_id_source"))).cast(LongType())) \
        .withColumn("customer_key", abs(hash(col("customer_id_source"))).cast(LongType())) \
        .withColumn("seller_key", abs(hash(col("seller_id_source"))).cast(LongType())) \
        .withColumn("order_key", abs(hash(col("order_id_source"))).cast(LongType())) \
        .withColumn("shipping_address_key", abs(hash(col("shipping_address_id_source"))).cast(LongType())) \
        .withColumn("billing_address_key", abs(hash(col("billing_address_id_source"))).cast(LongType())) \
        .withColumn("order_date_key", date_format(col("order_date_source"), "yyyyMMdd").cast(IntegerType()))
    print(f_sales_df)
    
    f_sales_df = f_sales_df.select(
        "order_item_id_source",
        "order_id_source",
        "order_date_key",
        "product_key",
        "customer_key",
        "seller_key",
        "order_key",
        "shipping_address_key",
        "billing_address_key",
        "quantity_sold",
        "unit_price_at_sale",
        "total_amount_item",
        "load_ts",
        "event_ts",
        "source_system"
    )

   
    
    write_to_clickhouse(f_sales_df, "f_sales", mode="append")
    print("Fact table f_sales processing complete.")


if __name__ == "__main__":
    print("Starting Spark batch processing...")
    process_dimensions()
    process_fact_table()
    spark.stop()
    print("Spark batch processing finished.")
