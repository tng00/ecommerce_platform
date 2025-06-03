import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, Error as OperationalError

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'password')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'default')

RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', 30))
RETRY_DELAY_SECONDS = int(os.getenv('RETRY_DELAY_SECONDS', 5))

TOPICS = [
    'inventory_adjustments',
    'payment_gateway_callbacks',
    'shipment_status_updates'
]

ch_client = None

print(f"Подключение к Kafka по адресу {KAFKA_BOOTSTRAP_SERVERS} и ClickHouse по адресу {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

def wait_for_kafka_connection():
    """Ожидает доступности брокеров Kafka."""
    for i in range(RETRY_ATTEMPTS):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.close()
            print("Успешно подключено к Kafka.")
            return True
        except NoBrokersAvailable:
            print(f"Попытка {i+1}/{RETRY_ATTEMPTS}: Kafka брокеры недоступны. Повторная попытка через {RETRY_DELAY_SECONDS} секунд...")
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            print(f"Попытка {i+1}/{RETRY_ATTEMPTS}: Неизвестная ошибка при подключении к Kafka: {e}. Повторная попытка через {RETRY_DELAY_SECONDS} секунд...")
            time.sleep(RETRY_DELAY_SECONDS)
    print("Не удалось подключиться к Kafka после нескольких попыток. Выход.")
    return False

def wait_for_clickhouse_connection():
    """Ожидает доступности ClickHouse и инициализирует клиент."""
    global ch_client
    for i in range(RETRY_ATTEMPTS):
        try:
            client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DB
            )
            client.execute('SELECT 1') 
            ch_client = client
            print("Успешно подключено к ClickHouse.")
            return True
        except (NetworkError, OperationalError) as e:
            print(f"Попытка {i+1}/{RETRY_ATTEMPTS}: ClickHouse недоступен или ошибка сети: {e}. Повторная попытка через {RETRY_DELAY_SECONDS} секунд...")
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            print(f"Попытка {i+1}/{RETRY_ATTEMPTS}: Неизвестная ошибка при подключении к ClickHouse: {e}. Повторная попытка через {RETRY_DELAY_SECONDS} секунд...")
            time.sleep(RETRY_DELAY_SECONDS)
    print("Не удалось подключиться к ClickHouse после нескольких попыток. Выход.")
    return False

def parse_timestamp(timestamp_data):
    """Парсит временную метку из различных форматов, возвращая объект datetime в UTC."""
    if isinstance(timestamp_data, (int, float)):
        return datetime.fromtimestamp(timestamp_data, tz=timezone.utc)
    elif isinstance(timestamp_data, str):
        try:
            return datetime.fromisoformat(timestamp_data).astimezone(timezone.utc)
        except ValueError:
            return None
    return None

def process_message(topic, message):
    try:
        data = json.loads(message.value.decode('utf-8'))
        
        if not data:
            print(f"Пропускаем сообщение из топика {topic}: полезная нагрузка пуста.")
            return

        order_id_source = data.get('order_id')
        event_timestamp = parse_timestamp(data.get('timestamp'))
        
        if order_id_source is None or event_timestamp is None:
            print(f"Пропускаем сообщение из топика {topic}: отсутствует 'order_id' или 'timestamp'. Сообщение: {data}")
            return

        record_to_insert = {
            'order_key': int(order_id_source), 
            'order_id_source': int(order_id_source),
            'customer_key': 0, 
            'order_date_key': 0, 
            'current_order_status': 'UNKNOWN',
            'current_order_status_updated_at': datetime(1970, 1, 1, tzinfo=timezone.utc),
            'shipping_address_key': 0,
            'billing_address_key': 0,
            'shipping_method': None,
            'current_tracking_number': None,
            'current_shipment_location': None,
            'current_payment_status': 'UNKNOWN',
            'payment_method': None,
            'last_transaction_id': None,
            'order_total_amount_source': 0.0, 
            'order_created_at_source': datetime(1970, 1, 1, tzinfo=timezone.utc), 
            'order_updated_at_source': event_timestamp, 
            'notes': None,
            'load_ts': datetime.now(timezone.utc),
            'source_system': 'producer_streaming'
        }
        
        if topic == 'inventory_adjustments':
            print(f"Получено сообщение о корректировке запасов для Order ID={order_id_source}. Нет прямого обновления полей d_orders для этого топика. Пропускаем вставку.")
            return 
            
        elif topic == 'payment_gateway_callbacks':
            status = data.get('status')
            transaction_id = data.get('transaction_id')

            if status is not None:
                record_to_insert['current_payment_status'] = str(status)
            if transaction_id is not None:
                record_to_insert['last_transaction_id'] = str(transaction_id)
            
            if event_timestamp > record_to_insert['current_order_status_updated_at']:
                record_to_insert['current_order_status_updated_at'] = event_timestamp
            
            record_to_insert['order_updated_at_source'] = event_timestamp 
            print(f"Обработка коллбэка платежа для Order ID={order_id_source}")

        elif topic == 'shipment_status_updates':
            new_status = data.get('new_status')
            tracking_number = data.get('tracking_number')
            location = data.get('location')

            if new_status is not None:
                record_to_insert['current_order_status'] = str(new_status)
            if tracking_number is not None:
                record_to_insert['current_tracking_number'] = str(tracking_number)
            if location is not None:
                record_to_insert['current_shipment_location'] = str(location)

            if event_timestamp > record_to_insert['current_order_status_updated_at']:
                record_to_insert['current_order_status_updated_at'] = event_timestamp

            record_to_insert['order_updated_at_source'] = event_timestamp
            print(f"Обработка обновления статуса отгрузки для Order ID={order_id_source}")
        
        else:
            print(f"Получено сообщение из необработанного топика: {topic}. Сообщение: {data}")
            return

        # Подготавливаем значения для вставки, обеспечивая правильный порядок и типы
        values_to_insert = (
            record_to_insert['order_key'],
            record_to_insert['order_id_source'],
            record_to_insert['customer_key'],
            record_to_insert['order_date_key'],
            record_to_insert['current_order_status'],
            record_to_insert['current_order_status_updated_at'],
            record_to_insert['shipping_address_key'],
            record_to_insert['billing_address_key'],
            record_to_insert['shipping_method'],
            record_to_insert['current_tracking_number'],
            record_to_insert['current_shipment_location'],
            record_to_insert['current_payment_status'],
            record_to_insert['payment_method'],
            record_to_insert['last_transaction_id'],
            float(record_to_insert['order_total_amount_source']), 
            record_to_insert['order_created_at_source'],
            record_to_insert['order_updated_at_source'],
            record_to_insert['notes'],
            record_to_insert['load_ts'],
            record_to_insert['source_system']
        )

        ch_client.execute(f"INSERT INTO d_orders VALUES", [values_to_insert])
        print(f"Вставлена новая запись в d_orders для Order ID: {order_id_source}")

    except json.JSONDecodeError as e:
        print(f"Ошибка декодирования JSON из топика {topic}: {e} - Сообщение: {message.value}")
    except ValueError as e:
        print(f"Ошибка преобразования типа данных в топике {topic}: {e} - Сообщение: {message.value}")
    except Exception as e:
        print(f"Произошла непредвиденная ошибка при обработке сообщения из топика {topic}: {e} - Сообщение: {message.value}")


def consume_messages():
    if not wait_for_kafka_connection() or not wait_for_clickhouse_connection():
        return

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: x, # Декодируем JSON вручную в process_message
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='clickhouse_producer_consumer'
    )
    print(f"Прослушивание сообщений в топиках: {TOPICS}")

    for message in consumer:
        process_message(message.topic, message)

if __name__ == "__main__":
    consume_messages()