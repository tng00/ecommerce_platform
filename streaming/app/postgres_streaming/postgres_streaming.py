import os
import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
from datetime import datetime, date

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPICS = [
    'pg_ecomm_cdc.public.addresses',
    'pg_ecomm_cdc.public.categories',
    'pg_ecomm_cdc.public.inventory_pg',
    'pg_ecomm_cdc.public.order_items',
    'pg_ecomm_cdc.public.orders',
    'pg_ecomm_cdc.public.payments',
    'pg_ecomm_cdc.public.products',
    'pg_ecomm_cdc.public.sellers',
    'pg_ecomm_cdc.public.users'
]
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'clickhouse_consumer_group')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'password')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'default')

def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

def transform_address(data):
    load_ts = datetime.utcnow()
    address_id_source = data['address_id']
    street_address = data['street_address']
    city_name_source = data['city']
    postal_code_source = data['postal_code']
    country_code_source = data['country_code']
    state_province = data['state_province']

    address_key = hash(f"{address_id_source}-{street_address}") % (2**63 - 1)
    city_key = hash(city_name_source) % (2**63 - 1)
    postal_code_key = hash(postal_code_source) % (2**63 - 1)
    country_key = hash(country_code_source) % (2**31 - 1)
    region_key = hash(f"{country_code_source}-{state_province}") % (2**31 - 1)

    return {
        'd_addresses': {
            'address_key': address_key,
            'address_id_source': address_id_source,
            'street_address': street_address,
            'city_key': city_key,
            'postal_code_key': postal_code_key,
            'load_ts': load_ts
        },
        'd_cities': {
            'city_key': city_key,
            'region_key': region_key,
            'city_name_source': city_name_source,
            'load_ts': load_ts
        },
        'd_postal_codes': {
            'postal_code_key': postal_code_key,
            'postal_code_source': postal_code_source,
            'load_ts': load_ts
        },
        'd_countries': {
            'country_key': country_key,
            'country_code_source': country_code_source,
            'country_name': country_code_source,
            'load_ts': load_ts
        },
        'd_regions': {
            'region_key': region_key,
            'country_key': country_key,
            'region_name_source': state_province,
            'load_ts': load_ts
        }
    }

def transform_category(data):
    load_ts = datetime.utcnow()
    category_id_source = data['category_id']
    category_name = data['name']
    parent_category_id = data['parent_category_id']
    parent_category_key = hash(parent_category_id) % (2**63 - 1) if parent_category_id is not None else None

    category_key = hash(category_id_source) % (2**63 - 1)

    return {
        'd_categories': {
            'category_key': category_key,
            'category_id_source': category_id_source,
            'category_name': category_name,
            'parent_category_key': parent_category_key,
            'load_ts': load_ts,
            'source_system': 'PostgreSQL'
        }
    }

def transform_product(data):
    load_ts = datetime.utcnow()
    product_id_source = data['product_id']
    seller_id = data['seller_id']
    category_id = data['category_id']
    product_name = data['name']
    description = data['description']
    sku = data['sku']

    product_key = hash(product_id_source) % (2**63 - 1)
    category_key = hash(category_id) % (2**63 - 1)
    seller_key = hash(seller_id) % (2**63 - 1)

    return {
        'd_products': {
            'product_key': product_key,
            'product_id_source': product_id_source,
            'product_name': product_name,
            'sku': sku,
            'category_key': category_key,
            'seller_key': seller_key,
            'description': description,
            'tags': [],
            'mongo_attributes': {},
            'load_ts': load_ts,
            'source_system': 'PostgreSQL'
        }
    }

def transform_seller(data):
    load_ts = datetime.utcnow()
    seller_id_source = data['seller_id']
    company_name = data['company_name']
    
    if isinstance(data['registration_date'], (int, float)):
        registration_date_ts = datetime.fromtimestamp(data['registration_date'] * 86400).date()
    elif isinstance(data['registration_date'], str):
        registration_date_ts = datetime.fromisoformat(data['registration_date'].replace('Z', '+00:00')).date()
    else:
        registration_date_ts = None

    is_active = 1 if data['is_active'] else 0

    seller_key = hash(seller_id_source) % (2**63 - 1)

    return {
        'd_sellers': {
            'seller_key': seller_key,
            'seller_id_source': seller_id_source,
            'company_name': company_name,
            'registration_date': registration_date_ts,
            'is_active': is_active,
            'load_ts': load_ts,
            'source_system': 'PostgreSQL'
        }
    }

def transform_user(data):
    load_ts = datetime.utcnow()
    customer_id_source = data['user_id']
    username = data['username']
    email = data['email']
    first_name = data['first_name']
    last_name = data['last_name']
    
    if isinstance(data['date_of_birth'], (int, float)):
        date_of_birth_ts = datetime.fromtimestamp(data['date_of_birth'] * 86400).date()
    elif isinstance(data['date_of_birth'], str):
        date_of_birth_ts = datetime.fromisoformat(data['date_of_birth'].replace('Z', '+00:00')).date()
    else:
        date_of_birth_ts = None
        
    default_shipping_address_id = data['default_shipping_address_id']
    default_billing_address_id = data['default_billing_address_id']
    
    registration_date_ts = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00')).date() if isinstance(data['created_at'], str) else None

    customer_key = hash(customer_id_source) % (2**63 - 1)
    default_shipping_address_key = hash(default_shipping_address_id) % (2**63 - 1) if default_shipping_address_id else None
    default_billing_address_key = hash(default_billing_address_id) % (2**63 - 1) if default_billing_address_id else None

    return {
        'd_customers': {
            'customer_key': customer_key,
            'customer_id_source': customer_id_source,
            'username': username,
            'email': email,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': date_of_birth_ts,
            'default_shipping_address_key': default_shipping_address_key,
            'default_billing_address_key': default_billing_address_key,
            'registration_date': registration_date_ts,
            'load_ts': load_ts,
            'source_system': 'PostgreSQL'
        }
    }

def transform_order(data):
    load_ts = datetime.utcnow()
    order_id_source = data['order_id']
    user_id = data['user_id']
    order_date_str = data['order_date']
    status = data['status']
    total_amount = float(data['total_amount'])
    shipping_address_id = data['shipping_address_id']
    billing_address_id = data['billing_address_id']
    shipping_method = data['shipping_method']
    tracking_number = data['tracking_number']
    notes = data['notes']
    created_at = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
    updated_at = datetime.fromisoformat(data['updated_at'].replace('Z', '+00:00'))

    order_date_key = int(datetime.fromisoformat(order_date_str.replace('Z', '+00:00')).strftime('%Y%m%d'))
    customer_key = hash(user_id) % (2**63 - 1)
    order_key = hash(order_id_source) % (2**63 - 1)
    shipping_address_key = hash(shipping_address_id) % (2**63 - 1)
    billing_address_key = hash(billing_address_id) % (2**63 - 1)

    return {
        'd_orders': {
            'order_key': order_key,
            'order_id_source': order_id_source,
            'customer_key': customer_key,
            'order_date_key': order_date_key,
            'current_order_status': status,
            'current_order_status_updated_at': updated_at,
            'shipping_address_key': shipping_address_key,
            'billing_address_key': billing_address_key,
            'shipping_method': shipping_method,
            'current_tracking_number': tracking_number,
            'current_shipment_location': None,
            'current_payment_status': status,
            'payment_method': None,
            'last_transaction_id': None,
            'order_total_amount_source': total_amount,
            'order_created_at_source': created_at,
            'order_updated_at_source': updated_at,
            'notes': notes,
            'load_ts': load_ts,
            'source_system': 'PostgreSQL'
        }
    }

def transform_order_item(data):
    load_ts = datetime.utcnow()
    order_item_id_source = data['order_item_id']
    order_id_source = data['order_id']
    product_id = data['product_id']
    quantity = data['quantity']
    unit_price = float(data['unit_price'])
    subtotal = float(data['subtotal'])
    event_ts = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))

    order_key = hash(order_id_source) % (2**63 - 1)
    product_key = hash(product_id) % (2**63 - 1)
    
    customer_key = hash(order_id_source) % (2**63 - 1)
    seller_key = hash(product_id) % (2**63 - 1)
    shipping_address_key = hash(order_id_source) % (2**63 - 1)
    billing_address_key = hash(order_id_source) % (2**63 - 1)
    
    order_date_key = int(event_ts.strftime('%Y%m%d'))

    return {
        'f_sales': {
            'order_item_id_source': order_item_id_source,
            'order_id_source': order_id_source,
            'order_date_key': order_date_key,
            'product_key': product_key,
            'customer_key': customer_key,
            'seller_key': seller_key,
            'order_key': order_key,
            'shipping_address_key': shipping_address_key,
            'billing_address_key': billing_address_key,
            'quantity_sold': quantity,
            'unit_price_at_sale': unit_price,
            'total_amount_item': subtotal,
            'load_ts': load_ts,
            'event_ts': event_ts,
            'source_system': 'PostgreSQL'
        }
    }

def transform_payment(data):
    load_ts = datetime.utcnow()
    order_id_source = data['order_id']
    payment_method = data['payment_method']
    transaction_id = data['transaction_id']
    status = data['status'] 

    order_key = hash(order_id_source) % (2**63 - 1)

    return {
        'd_orders_update': { 
            'order_key': order_key,
            'current_payment_status': status,
            'payment_method': payment_method,
            'last_transaction_id': transaction_id,
            'load_ts': load_ts
        }
    }

def process_message(topic, message_value, ch_client):
    op = message_value.get('op')
    if not op:
        print(f"Skipping message with no 'op' field on topic: {topic}. Raw message_value: {message_value}")
        return

    data = None
    if op in ['c', 'r', 'u']:
        data = message_value.get('after')
    elif op == 'd':
        deleted_data_id = message_value.get('before', {}).get('id')
        print(f"Skipping DELETE operation for topic: {topic}, ID: {deleted_data_id}. Full Debezium event: {message_value}")
        return
    else:
        print(f"Unknown operation type '{op}' on topic: {topic}. Full Debezium event: {message_value}")
        return
        
    if data is None:
        print(f"Skipping message with valid 'op' ({op}) but no 'after' data on topic: {topic}. Full Debezium event: {message_value}")
        return

    try:
        if 'pg_ecomm_cdc.public.addresses' in topic:
            transformed_data = transform_address(data)
            ch_client.execute(f"INSERT INTO d_addresses VALUES", [list(transformed_data['d_addresses'].values())])
            ch_client.execute(f"INSERT INTO d_cities VALUES", [list(transformed_data['d_cities'].values())])
            ch_client.execute(f"INSERT INTO d_postal_codes VALUES", [list(transformed_data['d_postal_codes'].values())])
            ch_client.execute(f"INSERT INTO d_countries VALUES", [list(transformed_data['d_countries'].values())])
            ch_client.execute(f"INSERT INTO d_regions VALUES", [list(transformed_data['d_regions'].values())])
            print(f"Inserted address data for address_id: {data.get('address_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.categories' in topic:
            transformed_data = transform_category(data)
            ch_client.execute(f"INSERT INTO d_categories VALUES", [list(transformed_data['d_categories'].values())])
            print(f"Inserted category data for category_id: {data.get('category_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.products' in topic:
            transformed_data = transform_product(data)
            ch_client.execute(f"INSERT INTO d_products VALUES", [list(transformed_data['d_products'].values())])
            print(f"Inserted product data for product_id: {data.get('product_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.sellers' in topic:
            transformed_data = transform_seller(data)
            ch_client.execute(f"INSERT INTO d_sellers VALUES", [list(transformed_data['d_sellers'].values())])
            print(f"Inserted seller data for seller_id: {data.get('seller_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.users' in topic:
            transformed_data = transform_user(data)
            ch_client.execute(f"INSERT INTO d_customers VALUES", [list(transformed_data['d_customers'].values())])
            print(f"Inserted customer data for user_id: {data.get('user_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.orders' in topic:
            transformed_data = transform_order(data)
            ch_client.execute(f"INSERT INTO d_orders VALUES", [list(transformed_data['d_orders'].values())])
            print(f"Inserted order data for order_id: {data.get('order_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.order_items' in topic:
            transformed_data = transform_order_item(data)
            ch_client.execute(f"INSERT INTO f_sales VALUES", [list(transformed_data['f_sales'].values())])
            print(f"Inserted sales data for order_item_id: {data.get('order_item_id')} (op: {op})")
        elif 'pg_ecomm_cdc.public.payments' in topic:
            transformed_data = transform_payment(data)
            print(f"Processing payment for order_id: {data.get('order_id')} (op: {op}) - Update d_orders with payment info.")
        elif 'pg_ecomm_cdc.public.inventory_pg' in topic:
            print(f"Skipping inventory_pg (op: {op}) for now, no direct DW mapping provided for f_inventory.")
        else:
            print(f"Received message from unhandled topic: {topic} (op: {op})")

    except Exception as e:
        print(f"Error processing data from topic {topic}: {data}. Error: {e}")

def main():
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode('utf-8') if m is not None else None,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )

    ch_client = get_clickhouse_client()
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} and ClickHouse at {CLICKHOUSE_PORT}")
    print(f"Listening to Kafka topics: {KAFKA_TOPICS} with group_id: {KAFKA_GROUP_ID}")

    try:
        for message in consumer:
            log_message_details = f"Topic='{message.topic}', Partition={message.partition}, Offset={message.offset}, Key='{message.key}'"

            if message.value is None:
                print(f"Skipping tombstone record (null value) for {log_message_details}")
                continue

            try:
                parsed_message_data = json.loads(message.value)
                process_message(message.topic, parsed_message_data, ch_client)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for {log_message_details}. Error: {e}. Raw value: {message.value}")
                continue
            except Exception as e:
                print(f"Unhandled error during message processing for {log_message_details}. Error: {e}")
                continue

    except KeyboardInterrupt:
        print("Stopping consumer due to KeyboardInterrupt.")
    except Exception as e:
        print(f"An unexpected error occurred in the main consumer loop: {e}")
    finally:
        consumer.close()
        print("Kafka consumer closed.")
        ch_client.disconnect()
        print("ClickHouse client disconnected.")

if __name__ == "__main__":
    main()