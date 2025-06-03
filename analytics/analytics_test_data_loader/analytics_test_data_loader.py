from clickhouse_driver import Client
from faker import Faker
from datetime import datetime, timedelta
import random

client = Client(host='clickhouse', user='default', password='password', port=9000)
fake = Faker()
now = datetime.now()

years = [(year, now) for year in range(2015, 2025)]
client.execute('INSERT INTO d_years VALUES', years)

months = []
for year in range(2015, 2025):
    for m in range(1, 13):
        month_key = year * 100 + m
        months.append((month_key, year, m, fake.month_name(), (m - 1) // 3 + 1, now))
client.execute('INSERT INTO d_months VALUES', months)

dates = []
start_date = datetime(2015, 1, 1)
end_date = datetime(2024, 12, 31)
total_days = (end_date - start_date).days

for i in range(500):
    random_days = random.randint(0, total_days)
    date_obj = start_date + timedelta(days=random_days)
    dates.append((
        int(date_obj.strftime('%Y%m%d')),
        int(date_obj.strftime('%Y%m')),
        date_obj.date(),
        date_obj.day,
        date_obj.isoweekday(),
        date_obj.strftime('%A'),
        date_obj.isocalendar().week,
        1 if date_obj.isoweekday() >= 6 else 0,
        now
    ))
existing_date_keys = [d[0] for d in dates]
client.execute('INSERT INTO d_date VALUES', dates)

countries = [(i, fake.country_code(), fake.country(), now) for i in range(1, 21)]
client.execute('INSERT INTO d_countries VALUES', countries)

regions = [(i, random.randint(1, 20), fake.state(), now) for i in range(1, 61)]
client.execute('INSERT INTO d_regions VALUES', regions)

cities = [(i, random.randint(1, 60), fake.city(), now) for i in range(1, 501)]
client.execute('INSERT INTO d_cities VALUES', cities)

postal_codes = [(i, fake.postcode(), now) for i in range(1, 501)]
client.execute('INSERT INTO d_postal_codes VALUES', postal_codes)

addresses = [(i, 10000 + i, fake.street_address(), random.randint(1, 500), random.randint(1, 500), now) for i in range(1, 501)]
client.execute('INSERT INTO d_addresses VALUES', addresses)

customers = []
for i in range(1, 501):
    customers.append((
        i, 2000 + i, fake.user_name(), fake.email(), fake.first_name(), fake.last_name(),
        fake.date_of_birth(minimum_age=18, maximum_age=70),
        random.randint(1, 500), random.randint(1, 500),
        fake.date_between(start_date='-5y', end_date='today'),
        now, 'PostgreSQL'
    ))
client.execute('INSERT INTO d_customers VALUES', customers)

sellers = [(i, 3000 + i, fake.company(), fake.date_between(start_date='-5y', end_date='today'), random.randint(0, 1), now, 'PostgreSQL') for i in range(1, 501)]
client.execute('INSERT INTO d_sellers VALUES', sellers)

categories = []
for i in range(1, 501):
    parent = None if i <= 50 else random.randint(1, 50)
    categories.append((i, 4000 + i, fake.word().capitalize(), parent, now, 'PostgreSQL'))
client.execute('INSERT INTO d_categories VALUES', categories)

products = []
for i in range(1, 501):
    products.append((
        i, 5000 + i, fake.word().capitalize(), f'SKU{i:05}', random.randint(1, 500),
        random.randint(1, 500), fake.text(30), ['tag1', 'tag2'],
        {'color': fake.color_name(), 'warranty': f"{random.randint(1, 3)}y"},
        now, 'Composite'
    ))
client.execute('INSERT INTO d_products VALUES', products)

reviews = []
for i in range(1, 501):
    review_id_source = f"review_{i:05}"
    product_key = random.randint(1, 500)
    customer_key = random.randint(1, 500)
    rating = random.randint(1, 5)
    title = fake.sentence(nb_words=6) if random.random() > 0.2 else None
    comment = fake.paragraph(nb_sentences=3) if random.random() > 0.1 else None
    image_urls = [fake.image_url() for _ in range(random.randint(0, 3))]
    is_verified_purchase = random.randint(0, 1)
    
    created_at = fake.date_time_between(start_date='-3y', end_date='-1y')
    updated_at = created_at + timedelta(days=random.randint(0, 100))

    reviews.append((
        i, review_id_source, product_key, customer_key, rating,
        title, comment, image_urls, is_verified_purchase,
        created_at, updated_at, now, 'MongoDB'
    ))

client.execute('''
    INSERT INTO d_reviews (
        review_key, review_id_source, product_key, customer_key, rating,
        title, comment, image_urls, is_verified_purchase,
        review_created_at_source, review_updated_at_source,
        load_ts, source_system
    ) VALUES
''', reviews)

sales = []
for i in range(1, 501):
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 500), 2)
    sales.append((
        10000 + i,
        20000 + i,
        random.choice(existing_date_keys),
        random.randint(1, 500),
        random.randint(1, 500),
        random.randint(1, 500),
        random.randint(1, 500),
        random.randint(1, 500),
        random.randint(1, 500),
        quantity,
        unit_price,
        round(quantity * unit_price, 2),
        now,
        now,
        'PostgreSQL'
    ))

client.execute('INSERT INTO f_sales VALUES', sales)

orders = []
for i in range(1, 501):
    order_key = i
    order_id_source = 100000 + i
    customer_key = random.randint(1, 500)
    order_date_key = random.choice(existing_date_keys)
    current_order_status = random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled'])
    current_order_status_updated_at = now - timedelta(days=random.randint(0, 10))
    shipping_address_key = random.randint(1, 500)
    billing_address_key = random.randint(1, 500)
    shipping_method = random.choice(['Standard', 'Express', 'Courier', None])
    current_tracking_number = fake.uuid4() if random.random() > 0.2 else None
    current_shipment_location = fake.city() if random.random() > 0.3 else None
    current_payment_status = random.choice(['pending', 'succeeded', 'failed'])
    payment_method = random.choice(['Credit Card', 'PayPal', 'Bank Transfer', None])
    last_transaction_id = fake.uuid4() if random.random() > 0.3 else None
    order_total_amount_source = round(random.uniform(20, 1000), 2)
    order_created_at_source = now - timedelta(days=random.randint(10, 3650))  # 10 лет в днях
    order_updated_at_source = order_created_at_source + timedelta(days=random.randint(0, 365))
    notes = fake.sentence() if random.random() > 0.7 else None
    load_ts = now
    source_system = 'PostgreSQL'

    orders.append((
        order_key, order_id_source, customer_key, order_date_key,
        current_order_status, current_order_status_updated_at,
        shipping_address_key, billing_address_key,
        shipping_method, current_tracking_number, current_shipment_location,
        current_payment_status, payment_method, last_transaction_id,
        order_total_amount_source, order_created_at_source, order_updated_at_source,
        notes, load_ts, source_system
    ))

client.execute('''
    INSERT INTO d_orders (
        order_key, order_id_source, customer_key, order_date_key,
        current_order_status, current_order_status_updated_at,
        shipping_address_key, billing_address_key,
        shipping_method, current_tracking_number, current_shipment_location,
        current_payment_status, payment_method, last_transaction_id,
        order_total_amount_source, order_created_at_source, order_updated_at_source,
        notes, load_ts, source_system
    ) VALUES
''', orders)

print("Успешно сгенерировано")