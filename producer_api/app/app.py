# producer_api/app/app.py
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import json
import time
import os
import logging
import atexit
from flasgger import Swagger
from flasgger.utils import swag_from

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
swagger = Swagger(app)

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL', 'localhost:29092')
producer = None

def initialize_kafka_producer():
    global producer
    if producer:
        logger.info("Kafka producer already initialized.")
        return
    try:
        logger.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER_URL}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            retries=5,
            acks='all',
        )
        logger.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER_URL}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka broker at {KAFKA_BROKER_URL}: {e}")
        producer = None

initialize_kafka_producer()

def close_kafka_producer():
    global producer
    if producer:
        logger.info("Closing Kafka producer...")
        try:
            producer.flush(timeout=10)
        except KafkaTimeoutError:
            logger.warning("Timeout while flushing messages on producer close.")
        except Exception as e:
            logger.error(f"Error flushing messages on producer close: {e}")
        finally:
            producer.close(timeout=10)
            producer = None
            logger.info("Kafka producer closed.")

atexit.register(close_kafka_producer)

def send_to_kafka(topic, key, data):
    global producer
    if not producer:
        logger.warning(f"Kafka producer is None. Attempting to re-initialize to {KAFKA_BROKER_URL}...")
        initialize_kafka_producer()
        if not producer:
            logger.error("Kafka producer is still not initialized after re-attempt. Cannot send message.")
            return False, "Kafka producer not available after re-initialization attempt"
    if 'timestamp' not in data:
        data['timestamp'] = time.time()
    try:
        logger.info(f"Sending message to topic '{topic}' with key '{key}': {data}")
        future = producer.send(topic, key=key, value=data)
        producer.flush(timeout=10)
        return True, "Message sent successfully"
    except KafkaTimeoutError:
        logger.error(f"Timeout error sending message to Kafka topic {topic}.")
        return False, f"Timeout error sending message to Kafka topic {topic}"
    except Exception as e:
        logger.error(f"Error sending message to Kafka topic {topic}: {e}")
        return False, str(e)


@app.route('/')
@swag_from('swagger_docs/index.yml')
def index():
    if producer:
        return f"Kafka Producer API is running! Connected to Kafka at {KAFKA_BROKER_URL}"
    else:
        return f"Kafka Producer API is running! ERROR: Not connected to Kafka at {KAFKA_BROKER_URL}. Check logs.", 503

@app.route('/produce/inventory_adjustments', methods=['POST'])
@swag_from('swagger_docs/produce_inventory_adjustments.yml')
def produce_inventory_adjustment():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    required_fields = ["product_id", "quantity_change", "reason"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": f"Missing one or more required fields: {', '.join(required_fields)}"}), 400

    product_id = data.get("product_id")
    success, message = send_to_kafka('inventory_adjustments', key=product_id, data=data)

    if success:
        return jsonify({"status": "success", "message": message, "data_sent": data}), 200
    else:
        return jsonify({"status": "error", "message": message}), 500


@app.route('/produce/payment_callbacks', methods=['POST'])
@swag_from('swagger_docs/produce_payment_callbacks.yml')
def produce_payment_callback():
    data = request.get_json()

    if not data:
        return jsonify({"error": "No data provided"}), 400

    required_fields = ["order_id", "transaction_id", "status", "gateway_data"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": f"Missing one or more required fields: {', '.join(required_fields)}"}), 400

    order_id = data.get("order_id")
    success, message = send_to_kafka('payment_gateway_callbacks', key=order_id, data=data)

    if success:
        return jsonify({"status": "success", "message": message, "data_sent": data}), 200
    else:
        return jsonify({"status": "error", "message": message}), 500

@app.route('/produce/shipment_updates', methods=['POST'])
@swag_from('swagger_docs/produce_shipment_updates.yml')
def produce_shipment_update():
    data = request.get_json()

    if not data:
        return jsonify({"error": "No data provided"}), 400

    required_fields = ["order_id", "tracking_number", "new_status"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": f"Missing one or more required fields: {', '.join(required_fields)}"}), 400

    order_id = data.get("order_id")
    success, message = send_to_kafka('shipment_status_updates', key=order_id, data=data)

    if success:
        return jsonify({"status": "success", "message": message, "data_sent": data}), 200
    else:
        return jsonify({"status": "error", "message": message}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('FLASK_RUN_PORT', 5001)), debug=os.environ.get('FLASK_DEBUG', '0') == '1')