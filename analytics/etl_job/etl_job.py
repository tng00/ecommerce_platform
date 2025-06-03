from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, count as spark_count, countDistinct,
    row_number, current_timestamp, when, corr, lag
)
from pyspark.sql.window import Window
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

def main():
    conf = SparkConf()
    conf.set("spark.jars.ivy", "/tmp/.ivy2")
    #conf.set("spark.jars.packages", "")
    conf.set("spark.jars", "/app/libs/clickhouse-jdbc-0.3.2-shaded.jar")
#    conf.set("spark.jars", "/opt/bitnami/spark/jars/clickhouse-jdbc-0.3.2-shaded.jar")
    spark = SparkSession.builder \
        .appName("Ecommerce Analytics ETL") \
        .master("spark://spark-master:7077") \
        .config(conf=conf) \
        .getOrCreate()

    jdbc_url = "jdbc:clickhouse://clickhouse:8123/default"
    props = {"user": "default", "password": "password", "driver": "ru.yandex.clickhouse.ClickHouseDriver"}

    f_sales     = spark.read.jdbc(jdbc_url, "f_sales",      properties=props)
    print("f_sales count:", f_sales.count())
    f_sales.printSchema() # Проверить схему, чтобы убедиться в правильности типов
    f_sales.show(5)


    d_products = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", """
        (
          SELECT
            product_key,
            product_name,
            category_key,
            seller_key,
            arrayStringConcat(tags, ',')                     AS tags_csv,
            toJSONString(mongo_attributes)                  AS mongo_attributes_json
          FROM d_products
        ) AS d_products_view 
    """) \
    .options(**props) \
    .load()
    print("d_products count:", d_products.count())
    d_products.printSchema()
    d_products.show(5)


    d_categories= spark.read.jdbc(jdbc_url, "d_categories", properties=props)

    # ------------------------------
    # ВИТРИНА 1.1: Топ-10 самых продаваемых продуктов
    # ------------------------------
    # Пример отладки:
    sales_by_product = (
        f_sales
        .groupBy("product_key")
        .agg(spark_sum("quantity_sold").alias("total_quantity_sold"))
        .join(d_products.select("product_key", "product_name"), "product_key")
    )
    sales_by_product.show() # <--- Добавьте это
    print(f"Sales by product count: {sales_by_product.count()}") # <--- И это

    window_prod = Window.orderBy(col("total_quantity_sold").desc())
    top10_products = (
        sales_by_product
        .withColumn("rank", row_number().over(window_prod))
        .filter(col("rank") <= 10)
        .drop("rank")
        .withColumn("load_ts", current_timestamp())
    )
    top10_products.show() # <--- Добавьте это
    print(f"Top 10 products count: {top10_products.count()}") # <--- И это

    top10_products.write.jdbc(
        url=jdbc_url,
        table="v_top_10_products_by_quantity",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 1.2: Общая выручка по категориям продуктов
    # ------------------------------
    revenue_by_product = (
        f_sales
        .groupBy("product_key")
        .agg(spark_sum("total_amount_item").alias("revenue"))
    )
    revenue_by_category = (
        revenue_by_product
        .join(d_products.select("product_key", "category_key"), "product_key")
        .groupBy("category_key")
        .agg(spark_sum("revenue").alias("category_total_revenue"))
        .join(d_categories.select("category_key", "category_name"), "category_key")
        .withColumn("load_ts", current_timestamp())
    )
    revenue_by_category.write.jdbc(
        url=jdbc_url,
        table="v_category_revenue",
        mode="append",
        properties=props
    )

    print("Первая витрина создана и загружена в ClickHouse.")

    f_sales    = spark.read.jdbc(jdbc_url, "f_sales", properties=props)
    d_customers= spark.read.jdbc(jdbc_url, "d_customers", properties=props)
    d_addresses= spark.read.jdbc(jdbc_url, "d_addresses", properties=props)
    d_cities   = spark.read.jdbc(jdbc_url, "d_cities", properties=props)
    d_regions  = spark.read.jdbc(jdbc_url, "d_regions", properties=props)
    d_countries= spark.read.jdbc(jdbc_url, "d_countries", properties=props)

    # ------------------------------
    # ВИТРИНА 2.1: Топ-10 клиентов по общей сумме покупок
    # ------------------------------
    customer_spent = (
        f_sales
        .groupBy("customer_key")
        .agg(spark_sum("total_amount_item").alias("total_spent"))
        .join(d_customers.select("customer_key","first_name","last_name"), "customer_key")
    )
    window_cust = Window.orderBy(col("total_spent").desc())
    top10_customers = (
        customer_spent
        .withColumn("rank", row_number().over(window_cust))
        .filter(col("rank") <= 10)
        .drop("rank")
        .withColumn("load_ts", current_timestamp())
    )
    top10_customers.write.jdbc(
        url=jdbc_url,
        table="v_top_10_customers_by_total_spent",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 2.2: Распределение клиентов по странам
    # ------------------------------
    cust_country = (
        d_customers
        .select("customer_key","default_shipping_address_key")
        .join(d_addresses.select("address_key","city_key"), 
              col("default_shipping_address_key")==col("address_key"), "left")
        .join(d_cities.select("city_key","region_key"), "city_key", "left")
        .join(d_regions.select("region_key","country_key"), "region_key", "left")
        .join(d_countries.select("country_key","country_name"), "country_key", "left")
        .groupBy("country_key","country_name")
        .agg(countDistinct("customer_key").alias("customer_count"))
        .withColumn("load_ts", current_timestamp())
    )
    cust_country.write.jdbc(
        url=jdbc_url,
        table="v_customer_country_distribution",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 2.3: Средний чек на клиента
    # ------------------------------
    order_totals = (
        f_sales
        .groupBy("customer_key","order_id_source")
        .agg(spark_sum("total_amount_item").alias("order_total"))
    )
    avg_check = (
        order_totals
        .groupBy("customer_key")
        .agg(spark_avg("order_total").alias("avg_order_value"))
        .join(d_customers.select("customer_key","first_name","last_name"), "customer_key")
        .withColumn("load_ts", current_timestamp())
    )
    avg_check.write.jdbc(
        url=jdbc_url,
        table="v_customer_avg_order_value",
        mode="append",
        properties=props
    )

    print("Вторая витрина загружена в ClickHouse.")

    f_sales   = spark.read.jdbc(jdbc_url, "f_sales",   properties=props)
    d_date    = spark.read.jdbc(jdbc_url, "d_date",    properties=props)
    d_months  = spark.read.jdbc(jdbc_url, "d_months",  properties=props)

    sales_with_month = (
        f_sales
        .join(d_date, f_sales.order_date_key == d_date.date_key)
        .join(d_months, d_date.month_key == d_months.month_key)
        .select(
            d_months.year_key,
            d_months.month_key,
            f_sales.order_id_source,
            f_sales.total_amount_item
        )
    )

    # ------------------------------
    # ВИТРИНА 3.1: Месячные и годовые тренды продаж
    # ------------------------------
    trends = (
        sales_with_month
        .groupBy("year_key", "month_key")
        .agg(spark_sum("total_amount_item").alias("total_revenue"))
        .withColumn("load_ts", current_timestamp())
    )
    trends.write.jdbc(
        url=jdbc_url,
        table="v_sales_trends",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 3.2: Сравнение выручки за разные периоды
    # ------------------------------
    month_window = Window.orderBy(col("month_key"))

    trends_with_lag = trends.select(
        "month_key",
        "total_revenue"
    ).withColumn(
        "prev_month_key", 
        lag("month_key").over(month_window)
    ).withColumn(
        "prev_revenue", 
        lag("total_revenue").over(month_window)
    ).filter(
        col("prev_month_key").isNotNull()
    )

    monthly_comparison = trends_with_lag.select(
        col("prev_month_key").alias("period_start_key"),
        col("month_key").alias("period_end_key"),
        col("prev_revenue").alias("revenue_start"),
        col("total_revenue").alias("revenue_end"),
        (col("total_revenue") - col("prev_revenue")).alias("revenue_diff"),
        current_timestamp().alias("load_ts")
    )

    monthly_comparison.write.jdbc(
        url=jdbc_url,
        table="v_sales_period_comparison",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 3.3: Средний размер заказа по месяцам
    # ------------------------------
    order_totals = (
        sales_with_month
        .groupBy("year_key", "month_key", "order_id_source")
        .agg(spark_sum("total_amount_item").alias("order_total"))
    )
    avg_monthly = (
        order_totals
        .groupBy("year_key", "month_key")
        .agg(spark_avg("order_total").alias("avg_order_value"))
        .withColumn("load_ts", current_timestamp())
    )
    avg_monthly.write.jdbc(
        url=jdbc_url,
        table="v_avg_order_value_monthly",
        mode="append",
        properties=props
    )

    print("Третья витрина загружена в ClickHouse.")

    f_sales    = spark.read.jdbc(jdbc_url, "f_sales",       properties=props)
    d_sellers  = spark.read.jdbc(jdbc_url, "d_sellers",     properties=props)
    d_addresses= spark.read.jdbc(jdbc_url, "d_addresses",   properties=props)
    d_cities   = spark.read.jdbc(jdbc_url, "d_cities",      properties=props)
    d_regions  = spark.read.jdbc(jdbc_url, "d_regions",     properties=props)
    d_countries= spark.read.jdbc(jdbc_url, "d_countries",   properties=props)

    # ------------------------------
    # ВИТРИНА 4.1: ТОП-5 МАГАЗИНОВ ПО ВЫРУЧКЕ
    # ------------------------------
    seller_revenue = (
        f_sales
        .groupBy("seller_key")
        .agg(spark_sum("total_amount_item").alias("total_revenue"))
        .join(d_sellers.select("seller_key", "company_name"), "seller_key")
    )
    window_sellers = Window.orderBy(col("total_revenue").desc())
    top5_sellers = (
        seller_revenue
        .withColumn("rank", row_number().over(window_sellers))
        .filter(col("rank") <= 5)
        .drop("rank")
        .withColumn("load_ts", current_timestamp())
    )
    top5_sellers.write.jdbc(
        url=jdbc_url,
        table="v_top_5_sellers_by_revenue",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 4.2: РАСПРЕДЕЛЕНИЕ ПРОДАЖ ПО ГОРОДАМ И СТРАНАМ
    # ------------------------------
    sales_loc = (
        f_sales
        .join(d_addresses, f_sales.shipping_address_key == d_addresses.address_key)
        .join(d_cities,    d_addresses.city_key == d_cities.city_key)
        .join(d_regions,   d_cities.region_key == d_regions.region_key)
        .join(d_countries, d_regions.country_key == d_countries.country_key)
        .groupBy(
            d_countries.country_key,
            d_countries.country_name,
            d_cities.city_key,
            d_cities.city_name_source.alias("city_name")
        )
        .agg(spark_sum("total_amount_item").alias("total_revenue"))
        .withColumn("load_ts", current_timestamp())
    )
    sales_loc.write.jdbc(
        url=jdbc_url,
        table="v_sales_distribution_by_location",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 4.3: СРЕДНИЙ ЧЕК ДЛЯ КАЖДОГО МАГАЗИНА
    # ------------------------------
    order_by_seller = (
        f_sales
        .groupBy("seller_key", "order_id_source")
        .agg(spark_sum("total_amount_item").alias("order_total"))
    )
    avg_check = (
        order_by_seller
        .groupBy("seller_key")
        .agg(spark_avg("order_total").alias("avg_order_value"))
        .join(d_sellers.select("seller_key", "company_name"), "seller_key")
        .withColumn("load_ts", current_timestamp())
    )
    avg_check.write.jdbc(
        url=jdbc_url,
        table="v_seller_avg_order_value",
        mode="append",
        properties=props
    )

    print("Четвёртая витрина загружена в ClickHouse.")

    f_sales = spark.read.jdbc(jdbc_url, "f_sales", properties=props)
    d_sellers = spark.read.jdbc(jdbc_url, "d_sellers", properties=props)
    d_products = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", """
        (
          SELECT
            product_key,
            product_name,
            category_key,
            seller_key,
            arrayStringConcat(tags, ',')                     AS tags_csv,
            toJSONString(mongo_attributes)                  AS mongo_attributes_json
          FROM d_products
        ) AS d_products_view 
    """) \
    .options(**props) \
    .load()

    # ------------------------------
    # ВИТРИНА 5.1: ТОП-5 ПОСТАВЩИКОВ ПО ВЫРЕЧКУ
    # ------------------------------
    v_top_5_suppliers_by_revenue = (
        f_sales.groupBy("seller_key")
        .agg(spark_sum("total_amount_item").alias("total_revenue"))
        .join(d_sellers.select("seller_key", "company_name"), on="seller_key")
        .orderBy(col("total_revenue").desc())
        .limit(5)
        .withColumn("load_ts", current_timestamp())
    )
    v_top_5_suppliers_by_revenue.write.jdbc(
        url=jdbc_url,
        table="v_top_5_suppliers_by_revenue",
        mode="append",
        properties=props
    )

    print("Пятая витрина загружена в ClickHouse.")

    f_sales = spark.read.jdbc(jdbc_url, "f_sales", properties=props)
    d_products = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", """
        (
          SELECT
            product_key,
            product_name,
            category_key,
            seller_key,
            arrayStringConcat(tags, ',')                     AS tags_csv,
            toJSONString(mongo_attributes)                  AS mongo_attributes_json
          FROM d_products
        ) AS d_products_view 
    """) \
    .options(**props) \
    .load()
    d_reviews = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", """
        (SELECT review_key, review_id_source, product_key, customer_key, rating, 
                title, comment, is_verified_purchase, 
                review_created_at_source, review_updated_at_source, load_ts, source_system
         FROM d_reviews) AS d_reviews_view
    """) \
    .options(**props) \
    .load()



    # ------------------------------
    # ВИТРИНА 6.1: ПРОДУКТЫ С НАИВЫСШИМ И НАИМЕНЬШИМ РЕЙТИНГОМ
    # ------------------------------
    product_ratings = (
        d_reviews.groupBy("product_key")
        .agg(spark_avg("rating").alias("avg_rating"))
        .join(d_products.select("product_key", "product_name"), on="product_key")
    )

    # Наивысший рейтинг
    v_products_best_rated = (
        product_ratings.orderBy(col("avg_rating").desc())
        .limit(1)
        .withColumn("rating_type", when(col("avg_rating") >= 0, "best"))
    )

    # Наименьший рейтинг
    v_products_worst_rated = (
        product_ratings.orderBy(col("avg_rating").asc())
        .limit(1)
        .withColumn("rating_type", when(col("avg_rating") >= 0, "worst"))
    )

    v_products_extreme_ratings = (
        v_products_best_rated.unionByName(v_products_worst_rated)
        .withColumn("load_ts", current_timestamp())
    )

    v_products_extreme_ratings.write.jdbc(
        url=jdbc_url,
        table="v_products_best_and_worst_rated",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 6.2: КОРРЕЛЯЦИЯ РЕЙТИНГА И ОБЪЕМА ПРОДАЖ
    # ------------------------------
    # Считаем среднюю оценку и общий объём продаж на продукт
   
    # 1) Средний рейтинг по продуктам
    avg_rating_df = d_reviews.groupBy("product_key").agg(
        spark_avg("rating").alias("avg_rating")
    )

    total_sales_df = f_sales.groupBy("product_key").agg(
        spark_sum("quantity_sold").alias("total_sales")
    )

    rating_sales = avg_rating_df.join(total_sales_df, on="product_key", how="inner")

    correlation_val = rating_sales.select(
        corr("avg_rating", "total_sales").alias("correlation")
    ).first()["correlation"]

    v_rating_sales_correlation = spark.createDataFrame([
        Row(
            correlation_value=correlation_val,
            load_ts=datetime.now()
        )
    ])

    v_rating_sales_correlation.write.jdbc(
        url=jdbc_url,
        table="v_rating_sales_correlation",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 6.3: ПРОДУКТЫ С НАИБОЛЬШИМ КОЛИЧЕСТВОМ ОТЗЫВОВ
    # ------------------------------
    v_products_most_reviewed = (
        d_reviews.groupBy("product_key")
        .agg(
            spark_count("*").alias("review_count"),
            spark_avg("rating").alias("avg_rating")
        )
        .join(d_products.select("product_key", "product_name"), on="product_key")
        .orderBy(col("review_count").desc())
        .limit(10)
        .withColumn("load_ts", current_timestamp())
    )

    v_products_most_reviewed.write.jdbc(
        url=jdbc_url,
        table="v_products_most_reviewed",
        mode="append",
        properties=props
    )

    print("Шестая витрина загружена в ClickHouse.")

    f_sales = spark.read.jdbc(jdbc_url, "f_sales", properties=props)
    d_orders = spark.read.jdbc(jdbc_url, "d_orders", properties=props)
    d_sellers = spark.read.jdbc(jdbc_url, "d_sellers", properties=props)
    d_date = spark.read.jdbc(jdbc_url, "d_date", properties=props)
    d_products = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", """
            (
            SELECT
                product_key,
                product_name,
                category_key,
                seller_key,
                arrayStringConcat(tags, ',') AS tags_csv,
                toJSONString(mongo_attributes) AS mongo_attributes_json
            FROM d_products
            ) AS d_products_view
        """) \
        .options(**props) \
        .load()
    
    # ------------------------------
    # ВИТРИНА 7.1: РАСПРЕДЕЛЕНИЕ ЗАКАЗОВ ПО СТАТУСАМ
    # ------------------------------
    v_order_status_distribution = (
    d_orders.groupBy("current_order_status")
    .agg(
        spark_count("*").alias("order_count"),
        spark_sum("order_total_amount_source").cast("decimal(18,2)").alias("total_revenue")
    )
    .withColumnRenamed("current_order_status", "order_status")
    .withColumn("load_ts", current_timestamp())
    )

    v_order_status_distribution.write.jdbc(
        url=jdbc_url,
        table="v_order_status_distribution",
        mode="append",
        properties=props
    )

    # ------------------------------
    # ВИТРИНА 7.2: КОЛИЧЕСТВО ЗАКАЗОВ И ВЫРУЧКА ПО ДНЯМ
    # ------------------------------
    v_daily_orders_and_revenue = (
        d_orders
        .join(d_date.select("date_key", "full_date"), d_orders.order_date_key == d_date.date_key)
        .groupBy("full_date")
        .agg(
            spark_count("*").alias("order_count"),
            spark_sum("order_total_amount_source").cast("decimal(18,2)").alias("total_revenue")
        )
        .withColumnRenamed("full_date", "order_date")
        .withColumn("load_ts", current_timestamp())
    )

    v_daily_orders_and_revenue.write.jdbc(
        url=jdbc_url,
        table="v_daily_orders_and_revenue",
        mode="append",
        properties=props
    )

    print("Седьмая витрина загружена в ClickHouse.")

    spark.stop()

if __name__ == "__main__":
    main()