from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime, timezone

conf = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'docker-producer-mongo'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

def generate_mongo_id():
    return f"{random.randint(0, 0xFFFFFFFFFFFFFFFFFFFF).to_bytes(12, 'big').hex()}"

def generate_test_message(topic):
    now_utc_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if topic == "mongo_ecomm_cdc.ecommerce_db.user_profiles_mongo":
        return {
            "_id": {"$oid": generate_mongo_id()},
            "userId": random.randint(1, 100),  
            "username": f"mongo_user_{random.randint(1, 100)}",
            "email": f"mongo_user{random.randint(1, 100)}@example.com",
            "firstName": random.choice(["John", "Jane", "Alice", "Bob"]),
            "lastName": random.choice(["Doe", "Smith", "Johnson", "Williams"]),
            "preferences": {"theme": random.choice(["dark", "light"]), "notifications": random.choice(["on", "off"])},
            "browseHistory": [
                {"productId": str(random.randint(1, 50)), "viewedAt": {"$date": now_utc_ms - random.randint(1, 3600000)}},
                {"productId": str(random.randint(1, 50)), "viewedAt": {"$date": now_utc_ms - random.randint(1, 3600000)}}
            ],
            "wishlist": [
                {"productId": str(random.randint(1, 50)), "addedAt": {"$date": now_utc_ms - random.randint(1, 3600000)}}
            ],
            "lastLoginIp": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "loyaltyPoints": random.randint(0, 1000),
            "createdAt": {"$date": now_utc_ms},
            "updatedAt": {"$date": now_utc_ms}
        }
    elif topic == "mongo_ecomm_cdc.ecommerce_db.product_details_mongo":
        return {
            "_id": {"$oid": generate_mongo_id()},
            "productId": str(random.randint(1, 50)),
            "name": f"Mongo Product {random.randint(1, 50)}",
            "description": "This is a detailed product description from MongoDB.",
            "sellerId": random.randint(1, 10), # Changed to integer
            "category": random.choice(["Electronics", "Books", "Clothing", "HomeGoods", "Toys"]),
            "tags": random.sample(["new", "popular", "sale", "limited", "eco-friendly"], k=random.randint(1, 3)),
            "variants": [
                {"sku": f"SKU{random.randint(10000, 99999)}-{random.choice(['A','B'])}",
                 "size": random.choice(["S", "M", "L", "XL"]),
                 "color": random.choice(["Red", "Blue", "Green", "Black"]),
                 "price": round(random.uniform(10.0, 500.0), 2),
                 "stock": random.randint(0, 200)}
            ],
            "attributes": [
                {"name": "Material", "value": random.choice(["Cotton", "Plastic", "Metal"])},
                {"name": "Weight", "value": f"{round(random.uniform(0.1, 5.0), 1)} kg"}
            ],
            "averageRating": round(random.uniform(2.5, 5.0), 1),
            "imageUrl": f"http://example.com/mongo_prod_{random.randint(1,10)}.jpg",
            "createdAt": {"$date": now_utc_ms},
            "updatedAt": {"$date": now_utc_ms}
        }
    elif topic == "mongo_ecomm_cdc.ecommerce_db.seller_profiles_mongo":
        return {
            "_id": {"$oid": generate_mongo_id()},
            "sellerId": random.randint(1, 10), # Changed to integer
            "companyName": f"MongoSellerCo {random.randint(1, 10)}",
            "profileDescription": "We are a top-rated seller on the MongoDB platform.",
            "logoUrl": f"http://example.com/seller_logo_{random.randint(1,10)}.png",
            "bannerUrl": f"http://example.com/seller_banner_{random.randint(1,10)}.png",
            "socialMediaLinks": {"facebook": "fb.com/mongoseller", "twitter": "twitter.com/mongoseller"},
            "returnPolicy": "60-day hassle-free returns.",
            "customerServiceContact": {"email": f"support_seller{random.randint(1,10)}@example.com", "phone": f"1-800-{random.randint(100,999)}-{random.randint(1000,9999)}"},
            "averageRating": round(random.uniform(3.0, 5.0), 1),
            "isActive": random.choice([True, False]),
            "createdAt": {"$date": now_utc_ms},
            "updatedAt": {"$date": now_utc_ms}
        }
    elif topic == "mongo_ecomm_cdc.ecommerce_db.reviews_mongo":
        return {
            "_id": {"$oid": generate_mongo_id()},
            "reviewId": str(random.randint(1, 1000)),
            "productId": str(random.randint(1, 50)),
            "userId": random.randint(1, 100),  # Changed to integer
            "rating": random.randint(1, 5),
            "title": random.choice(["Excellent!", "Disappointed", "Just Okay", "Highly Recommend"]),
            "comment": "This is a detailed review comment for the product. " + random.choice(["Loved it!", "Could be improved.", "Worth the price."]),
            "imageUrls": [f"http://example.com/review_img_{random.randint(1,5)}.jpg" for _ in range(random.randint(0,2))],
            "isVerifiedPurchase": random.choice([True, False]),
            "helpfulVotes": random.randint(0, 100),
            "createdAt": {"$date": now_utc_ms},
            "updatedAt": {"$date": now_utc_ms}
        }
    else:
        now_utc_iso = datetime.now(timezone.utc).isoformat(timespec='microseconds')
        if topic == "pg_ecomm_cdc.public.orders":
            return {
                "order_id": random.randint(1000, 9999),
                "user_id": random.randint(1, 100),
                "customer_id": f"user_{random.randint(1, 100)}",
                "product_id": random.randint(1, 50),
                "quantity": random.randint(1, 5),
                "order_date": now_utc_iso,
                "status": random.choice(["pending", "shipped", "delivered"]),
                "total_amount": round(random.uniform(10.0, 500.0), 2),
                "shipping_address_id": random.randint(100, 999),
                "billing_address_id": random.randint(100, 999),
                "shipping_method": random.choice(["standard", "express", "overnight"]),
                "tracking_number": f"TRACK{random.randint(100000, 999999)}",
                "notes": "Sample order note",
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.users":
            return {
                "user_id": random.randint(1, 100),
                "username": f"user_{random.randint(1, 100)}",
                "email": f"user{random.randint(1, 100)}@example.com",
                "first_name": random.choice(["John", "Jane", "Peter", "Susan"]),
                "last_name": random.choice(["Doe", "Smith", "Jones", "Williams"]),
                "date_of_birth": (datetime.now().year - random.randint(18, 60)).__str__() + "-01-01",
                "phone_number": f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.products":
            return {
                "product_id": random.randint(1, 50),
                "seller_id": random.randint(1, 10),
                "category_id": random.randint(1, 5),
                "name": f"Product {random.randint(1, 50)}",
                "description": "This is a sample product description.",
                "sku": f"SKU{random.randint(10000, 99999)}",
                "base_price": round(random.uniform(5.0, 1000.0), 2),
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.inventory_pg":
            return {
                "inventory_id": random.randint(1, 1000),
                "product_id": random.randint(1, 50),
                "quantity": random.randint(0, 200),
                "reserved_quantity": random.randint(0, 50),
                "last_restock_date": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.addresses":
            return {
                "address_id": random.randint(100, 999),
                "user_id": random.randint(1, 100),
                "street_address": f"{random.randint(1, 999)} Main St",
                "city": random.choice(["Anytown", "Otherville", "Somewhere"]),
                "state_province": random.choice(["CA", "NY", "TX"]),
                "postal_code": f"{random.randint(10000, 99999)}",
                "country_code": "US",
                "address_type": random.choice(["shipping", "billing"]),
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.sellers":
            return {
                "seller_id": random.randint(1, 10),
                "user_id": random.randint(1, 100),
                "company_name": f"SellerCo {random.randint(1, 10)}",
                "tax_id": f"TAX{random.randint(100000000, 999999999)}",
                "registration_date": now_utc_iso,
                "is_active": random.choice([True, False]),
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.categories":
            return {
                "category_id": random.randint(1, 5),
                "name": random.choice(["Electronics", "Books", "Clothing", "Home", "Sports"]),
                "parent_category_id": None if random.random() > 0.5 else random.randint(1, 5),
                "description": "Category description",
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.order_items":
            return {
                "order_item_id": random.randint(10000, 99999),
                "order_id": random.randint(1000, 9999),
                "product_id": random.randint(1, 50),
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(5.0, 500.0), 2),
                "subtotal": round(random.uniform(10.0, 2500.0), 2),
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "pg_ecomm_cdc.public.payments":
            return {
                "payment_id": random.randint(10000, 99999),
                "order_id": random.randint(1000, 9999),
                "payment_date": now_utc_iso,
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
                "transaction_id": f"TRANS{random.randint(10000000, 99999999)}",
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "status": random.choice(["completed", "failed", "pending"]),
                "created_at": now_utc_iso,
                "updated_at": now_utc_iso
            }
        elif topic == "inventory_adjustments":
            return {
                "product_id": random.randint(1, 50),
                "quantity_change": random.choice([-10, -5, 5, 10]),
                "reason": random.choice(["stock_take", "return", "damage"]),
                "timestamp": now_utc_iso
            }
        elif topic == "payment_gateway_callbacks":
            return {
                "order_id": random.randint(1000, 9999),
                "transaction_id": f"TXN{random.randint(100000000, 999999999)}",
                "status": random.choice(["success", "failure", "pending"]),
                "gateway_data": {"payment_processor": random.choice(["stripe", "paypal"]), "amount_paid": round(random.uniform(10.0, 1000.0), 2)},
                "timestamp": now_utc_iso
            }
        elif topic == "shipment_status_updates":
            return {
                "order_id": random.randint(1000, 9999),
                "tracking_number": f"SHIP{random.randint(1000000, 9999999)}",
                "new_status": random.choice(["shipped", "in_transit", "delivered", "exception"]),
                "location": random.choice(["Warehouse A", "Distribution Center B", "Customer Address"]),
                "timestamp": now_utc_iso
            }
        else:
            return {
                "message": f"Test message for {topic}",
                "timestamp": now_utc_iso,
                "random_value": random.random()
            }

topics = [
    "pg_ecomm_cdc.public.orders",
    "pg_ecomm_cdc.public.users",
    "pg_ecomm_cdc.public.products",
    "pg_ecomm_cdc.public.inventory_pg",
    "pg_ecomm_cdc.public.addresses",
    "pg_ecomm_cdc.public.sellers",
    "pg_ecomm_cdc.public.categories",
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

for topic in topics:
    print(f"--- Producing messages for topic: {topic} ---")
    num_messages = 10 if topic.startswith("mongo_ecomm_cdc") else 5
    for i in range(num_messages):
        message_data = generate_test_message(topic)
        
        if topic.startswith("pg_ecomm_cdc") or topic.startswith("mongo_ecomm_cdc"):
            message_payload = {
                "before": None,
                "after": message_data,
                "source": {
                    "version": "generator_mock",
                    "connector": "generator",
                    "ts_ms": int(time.time() * 1000)
                },
                "op": "c",
                "ts_ms": int(time.time() * 1000)
            }
        else:
            message_payload = message_data

        producer.produce(
            topic=topic,
            key=str(random.randint(0, 1000)),
            value=json.dumps(message_payload, default=str).encode('utf-8'),
            callback=delivery_report
        )
        print(f"Sent to {topic}: {json.dumps(message_payload, indent=2, default=str)}")
        time.sleep(0.5)

producer.flush()

print("Finished sending all test messages")