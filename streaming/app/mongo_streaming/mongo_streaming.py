import json
import os
from datetime import datetime, date
from kafka import KafkaConsumer
from clickhouse_driver import Client
from dateutil.parser import parse
import hashlib

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'clickhouse_mongo_consumer_group')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'password')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'default')

KAFKA_TOPICS = [
    'mongo_ecomm_cdc.ecommerce_db.user_profiles_mongo',
    'mongo_ecomm_cdc.ecommerce_db.product_details_mongo',
    'mongo_ecomm_cdc.ecommerce_db.seller_profiles_mongo',
    'mongo_ecomm_cdc.ecommerce_db.reviews_mongo'
]

def get_clickhouse_client():
    """Устанавливает и возвращает клиент ClickHouse."""
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

def generate_key(source_id, prefix=""):
    """
    Генерирует согласованный ключ UInt64 из исходного ID (строки или ObjectId)
    используя SHA256 хеш.
    """
    if source_id is None:
        return 0
    key_string = f"{prefix}-{str(source_id)}"
    hasher = hashlib.sha256(key_string.encode('utf-8'))
    return int(hasher.hexdigest()[:16], 16)

def parse_mongo_date(mongo_date_obj):
    """
    Парсит объект MongoDB $date (миллисекунды Unix эпохи или ISO строку)
    в объект datetime.
    """
    if isinstance(mongo_date_obj, dict) and '$date' in mongo_date_obj:
        date_val = mongo_date_obj['$date']
        if isinstance(date_val, (int, float)):
            return datetime.fromtimestamp(date_val / 1000)
        elif isinstance(date_val, str):
            try:
                return parse(date_val)
            except ValueError:
                print(f"Предупреждение: Не удалось разобрать строку даты: {date_val}")
                return None
    elif isinstance(mongo_date_obj, str):
        try:
            return parse(mongo_date_obj)
        except ValueError:
            print(f"Предупреждение: Не удалось разобрать строку даты: {mongo_date_obj}")
            return None
    return None

def transform_customer(data):
    """Преобразует данные пользователей MongoDB для d_customers."""
    load_ts = datetime.utcnow()
    customer_id_source = data.get('userId')
    username = data.get('username')
    email = data.get('email')
    first_name = data.get('firstName')
    last_name = data.get('lastName')
    date_of_birth = None 
    
    default_shipping_address_key = None 
    default_billing_address_key = None

    registration_date = parse_mongo_date(data.get('createdAt'))
    
    customer_key = generate_key(customer_id_source, 'customer')

    return {
        'd_customers': {
            'customer_key': customer_key,
            'customer_id_source': str(customer_id_source),
            'username': username,
            'email': email,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': date_of_birth,
            'default_shipping_address_key': default_shipping_address_key,
            'default_billing_address_key': default_billing_address_key,
            'registration_date': registration_date.date() if registration_date else None,
            'load_ts': load_ts,
            'source_system': 'MongoDB'
        }
    }

def transform_seller(data):
    """Преобразует данные продавцов MongoDB для d_sellers."""
    load_ts = datetime.utcnow()
    seller_id_source = data.get('sellerId')
    company_name = data.get('companyName')
    registration_date = parse_mongo_date(data.get('createdAt'))
    is_active = 1 
    if 'isActive' in data:
        is_active = 1 if data['isActive'] else 0

    seller_key = generate_key(seller_id_source, 'seller')

    return {
        'd_sellers': {
            'seller_key': seller_key,
            'seller_id_source': str(seller_id_source),
            'company_name': company_name,
            'registration_date': registration_date.date() if registration_date else None,
            'is_active': is_active,
            'load_ts': load_ts,
            'source_system': 'MongoDB'
        }
    }

def transform_product(data):
    """Преобразует данные продуктов MongoDB для d_products и d_categories."""
    load_ts = datetime.utcnow()
    product_id_source = data.get('productId')
    seller_id = data.get('sellerId')
    
    product_name = data.get('name')
    sku = data.get('variants', [{}])[0].get('sku')
    description = data.get('description')
    tags = data.get('tags', [])
    
    mongo_attributes = {}
    attributes_list = data.get('attributes', [])
    for attr in attributes_list:
        if isinstance(attr, dict) and 'name' in attr and 'value' in attr:
            mongo_attributes[attr['name']] = str(attr['value'])

    category_name = data.get('category')
    category_key = generate_key(category_name, 'category')

    product_key = generate_key(product_id_source, 'product')
    seller_key = generate_key(seller_id, 'seller')

    return {
        'd_products': {
            'product_key': product_key,
            'product_id_source': str(product_id_source),
            'product_name': product_name,
            'sku': sku,
            'category_key': category_key,
            'seller_key': seller_key,
            'description': description,
            'tags': tags,
            'mongo_attributes': mongo_attributes,
            'load_ts': load_ts,
            'source_system': 'MongoDB'
        },
        'd_categories': {
            'category_key': category_key,
            'category_id_source': 0,
            'category_name': category_name,
            'parent_category_key': None,
            'load_ts': load_ts,
            'source_system': 'MongoDB'
        }
    }

def transform_review(data):
    """Преобразует данные отзывов MongoDB для d_reviews."""
    load_ts = datetime.utcnow()
    review_id_source = data.get('reviewId')
    product_id = data.get('productId')
    customer_id = data.get('userId')
    rating = data.get('rating')
    title = data.get('title')
    comment = data.get('comment')
    image_urls = data.get('imageUrls', [])
    is_verified_purchase = 1 if data.get('verifiedPurchase') else 0

    review_created_at_source = parse_mongo_date(data.get('createdAt'))
    review_updated_at_source = parse_mongo_date(data.get('updatedAt'))

    review_key = generate_key(review_id_source, 'review')
    product_key = generate_key(product_id, 'product')
    customer_key = generate_key(customer_id, 'customer')

    return {
        'd_reviews': {
            'review_key': review_key,
            'review_id_source': str(review_id_source),
            'product_key': product_key,
            'customer_key': customer_key,
            'rating': rating,
            'title': title,
            'comment': comment,
            'image_urls': image_urls,
            'is_verified_purchase': is_verified_purchase,
            'review_created_at_source': review_created_at_source,
            'review_updated_at_source': review_updated_at_source,
            'load_ts': load_ts,
            'source_system': 'MongoDB'
        }
    }


def process_message(topic, message_value, ch_client):
    """Обрабатывает одно сообщение Kafka и записывает в ClickHouse."""
    try:
        payload = message_value
        
        data_to_process = None
        if 'op' in payload and 'after' in payload:
            data_to_process = payload['after']
        else:
            data_to_process = payload

        if not data_to_process:
            print(f"Пропускаем сообщение из топика {topic}: полезная нагрузка (после 'after' или сам payload) пуста или некорректна.")
            return


        if 'mongo_ecomm_cdc.ecommerce_db.user_profiles_mongo' in topic:
            transformed_data = transform_customer(data_to_process)
            if transformed_data: 
                ch_client.execute(f"INSERT INTO d_customers VALUES", [list(transformed_data['d_customers'].values())])
                print(f"Вставлены данные клиента (Mongo) для userId: {data_to_process.get('userId')}")

        elif 'mongo_ecomm_cdc.ecommerce_db.product_details_mongo' in topic:
            transformed_data = transform_product(data_to_process)
            if transformed_data: 
                category_data = transformed_data['d_categories']
                
                category_exists = ch_client.execute(f"""
                    SELECT 1 FROM d_categories WHERE category_key = %s LIMIT 1
                    """,
                    [category_data['category_key']] 
                )
                
                if not category_exists:
                    ch_client.execute(f"""
                        INSERT INTO d_categories (
                            category_key, category_id_source, category_name, parent_category_key, load_ts, source_system
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        [(
                            category_data['category_key'], category_data['category_id_source'],
                            category_data['category_name'], category_data['parent_category_key'],
                            category_data['load_ts'], category_data['source_system']
                        )]
                    )
                    print(f"Вставлена новая категория (Mongo): {data_to_process.get('category')}")
                else:
                    print(f"Категория '{data_to_process.get('category')}' (Mongo) уже существует. Пропускаем вставку.")

                ch_client.execute(f"INSERT INTO d_products VALUES", [list(transformed_data['d_products'].values())])
                print(f"Вставлены данные продукта (Mongo) для productId: {data_to_process.get('productId')}")


        elif 'mongo_ecomm_cdc.ecommerce_db.seller_profiles_mongo' in topic:
            transformed_data = transform_seller(data_to_process)
            if transformed_data: 
                ch_client.execute(f"INSERT INTO d_sellers VALUES", [list(transformed_data['d_sellers'].values())])
                print(f"Вставлены данные продавца (Mongo) для sellerId: {data_to_process.get('sellerId')}")

        elif 'mongo_ecomm_cdc.ecommerce_db.reviews_mongo' in topic:
            transformed_data = transform_review(data_to_process)
            if transformed_data: 
                ch_client.execute(f"INSERT INTO d_reviews VALUES", [list(transformed_data['d_reviews'].values())])
                print(f"Вставлены данные отзыва (Mongo) для reviewId: {data_to_process.get('reviewId')}")

        else:
            print(f"Получено сообщение из необработанного топика: {topic}")

    except Exception as e:
        print(f"Ошибка при обработке сообщения из топика {topic}: {message_value}. Ошибка: {e}")


def main():
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )

    ch_client = get_clickhouse_client()
    print(f"Подключение к Kafka по адресу {KAFKA_BOOTSTRAP_SERVERS} и ClickHouse по адресу {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    print(f"Прослушивание Kafka топиков: {KAFKA_TOPICS} с group_id: {KAFKA_GROUP_ID}")

    try:
        for message in consumer:
            if message.value:
                process_message(message.topic, message.value, ch_client)
            else:
                print(f"Получено пустое сообщение из топика: {message.topic}")

    except KeyboardInterrupt:
        print("Остановка консьюмера.")
    finally:
        consumer.close()
        print("Kafka консьюмер закрыт.")
        ch_client.disconnect()
        print("ClickHouse клиент отключен.")

if __name__ == "__main__":
    main()