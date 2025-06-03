-- ПЕРВАЯ ВИТРИНА
DROP TABLE IF EXISTS v_top_10_products_by_quantity;
DROP TABLE IF EXISTS v_category_revenue;

-- ВТОРАЯ ВИТРИНА
DROP TABLE IF EXISTS v_top_10_customers_by_total_spent;
DROP TABLE IF EXISTS v_customer_country_distribution;
DROP TABLE IF EXISTS v_customer_avg_order_value;

-- ТРЕТЬЯ ВИТРИНА
DROP TABLE IF EXISTS v_sales_trends;
DROP TABLE IF EXISTS v_sales_period_comparison;
DROP TABLE IF EXISTS v_avg_order_value_monthly;

-- ЧЕТВЁРТАЯ ВИТРИНА
DROP TABLE IF EXISTS v_top_5_sellers_by_revenue;
DROP TABLE IF EXISTS v_sales_distribution_by_location;
DROP TABLE IF EXISTS v_seller_avg_order_value;

-- ПЯТАЯ ВИТРИНА
DROP TABLE IF EXISTS v_top_5_suppliers_by_revenue;

-- ШЕСТАЯ ВИТРИНА
DROP TABLE IF EXISTS v_products_best_and_worst_rated;
DROP TABLE IF EXISTS v_rating_sales_correlation;
DROP TABLE IF EXISTS v_products_most_reviewed;

--------------- ПЕРВАЯ ВИТРИНА ---------------
-- 1) Топ-10 самых продаваемых продуктов
CREATE TABLE v_top_10_products_by_quantity
(
    product_key UInt64,
    product_name String,
    total_quantity_sold UInt64,
    load_ts       DateTime64
)
ENGINE = MergeTree()
ORDER BY total_quantity_sold;

-- 2) Общая выручка по категориям продуктов
CREATE TABLE v_category_revenue
(
    category_key            UInt64,
    category_name           String,
    category_total_revenue  Decimal(18,2),
    load_ts                 DateTime64
)
ENGINE = MergeTree()
ORDER BY category_total_revenue;

--------------- ВТОРАЯ ВИТРИНА ---------------
-- 1) Топ-10 клиентов с наибольшей общей суммой покупок
CREATE TABLE v_top_10_customers_by_total_spent
(
    customer_key   UInt64,
    first_name     String,
    last_name      String,
    total_spent    Decimal(18,2),
    load_ts        DateTime64
)
ENGINE = MergeTree()
ORDER BY total_spent;

-- 2) Распределение клиентов по странам
CREATE TABLE v_customer_country_distribution
(
    country_key       UInt32,
    country_name      String,
    customer_count    UInt32,
    load_ts           DateTime64
)
ENGINE = MergeTree()
ORDER BY customer_count;

-- 3) Средний чек для каждого клиента
CREATE TABLE v_customer_avg_order_value
(
    customer_key      UInt64,
    first_name        String,
    last_name         String,
    avg_order_value   Decimal(18,2),
    load_ts           DateTime64
)
ENGINE = MergeTree()
ORDER BY avg_order_value;

--------------- ТРЕТЬЯ ВИТРИНА ---------------
-- 1) Месячные и годовые тренды продаж
CREATE TABLE v_sales_trends
(
    year_key        UInt16,
    month_key       UInt32,
    total_revenue   Decimal(18,2),
    load_ts         DateTime64
)
ENGINE = MergeTree()
ORDER BY (year_key, month_key);

-- 2) Сравнение выручки за разные периоды
CREATE TABLE v_sales_period_comparison
(
    period_start_key   UInt32,    
    period_end_key     UInt32,    
    revenue_start      Decimal(18,2),
    revenue_end        Decimal(18,2),
    revenue_diff       Decimal(18,2),  
    load_ts            DateTime64
)
ENGINE = MergeTree()
ORDER BY (period_start_key, period_end_key);

-- 3) Средний размер заказа по месяцам
CREATE TABLE v_avg_order_value_monthly
(
    year_key           UInt16,
    month_key          UInt32,
    avg_order_value    Decimal(18,2),
    load_ts            DateTime64
)
ENGINE = MergeTree()
ORDER BY (year_key, month_key);

--------------- ЧЕТВЁРТАЯ ВИТРИНА ---------------
-- 1) Топ-5 магазинов (продавцов) с наибольшей выручкой
CREATE TABLE v_top_5_sellers_by_revenue
(
    seller_key        UInt64,
    company_name      String,
    total_revenue     Decimal(18,2),
    load_ts           DateTime64
)
ENGINE = MergeTree()
ORDER BY total_revenue;

-- 2) Распределение продаж по городам и странам
CREATE TABLE v_sales_distribution_by_location
(
    country_key       UInt32,
    country_name      String,
    city_key          UInt64,
    city_name         String,
    total_revenue     Decimal(18,2),
    load_ts           DateTime64
)
ENGINE = MergeTree()
ORDER BY (country_key, city_key);

-- 3) Средний чек для каждого магазина (продавца)
CREATE TABLE v_seller_avg_order_value
(
    seller_key        UInt64,
    company_name      String,
    avg_order_value   Decimal(18,2),
    load_ts           DateTime64
)
ENGINE = MergeTree()
ORDER BY avg_order_value;

--------------- ПЯТАЯ ВИТРИНА ---------------
-- 1) Топ-5 поставщиков с наибольшей выручкой
CREATE TABLE v_top_5_suppliers_by_revenue
(
    seller_key     UInt64,         
    company_name   String,
    total_revenue  Decimal(18,2),
    load_ts        DateTime64
)
ENGINE = MergeTree()
ORDER BY total_revenue;

--------------- ШЕСТАЯ ВИТРИНА ---------------
-- 1) Распределение заказов по статусам
CREATE TABLE v_products_best_and_worst_rated
(
    product_key     UInt64,
    product_name    String,
    avg_rating      Float32,
    rating_type     Enum8('best' = 1, 'worst' = 2),
    load_ts         DateTime64
)
ENGINE = MergeTree()
ORDER BY (rating_type, avg_rating);

-- 2) Корреляция между рейтингом и объёмом продаж
CREATE TABLE v_rating_sales_correlation
(
    correlation_value   Float32,
    method              Enum8('pearson' = 1, 'spearman' = 2),
    load_ts             DateTime64
)
ENGINE = TinyLog();

-- 3) Продукты с наибольшим количеством отзывов
CREATE TABLE v_products_most_reviewed
(
    product_key       UInt64,
    product_name      String,
    review_count      UInt32,
    avg_rating        Float32,
    load_ts           DateTime64
)
ENGINE = MergeTree()
ORDER BY review_count;

--------------- СЕДЬМАЯ ВИТРИНА ---------------
-- 1) Распределение заказов по статусам
CREATE TABLE IF NOT EXISTS v_order_status_distribution
(
    order_status     String,
    order_count      UInt64,
    total_revenue    Decimal(18,2),
    load_ts          DateTime64
)
ENGINE = MergeTree()
ORDER BY order_count;

-- 2) Количество заказов и выручка по дням
CREATE TABLE IF NOT EXISTS v_daily_orders_and_revenue
(
    order_date       Date,
    order_count      UInt64,
    total_revenue    Decimal(18,2),
    load_ts          DateTime64
)
ENGINE = MergeTree()
ORDER BY order_date;