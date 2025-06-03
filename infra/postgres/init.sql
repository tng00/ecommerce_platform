-- postgres/init.sql

CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE users (
  user_id SERIAL PRIMARY KEY,
  username VARCHAR(255) UNIQUE NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  date_of_birth DATE,
  phone_number VARCHAR(20),
  default_shipping_address_id INTEGER,  
  default_billing_address_id INTEGER,   
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE users REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_users
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE addresses (
  address_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
  street_address VARCHAR(255) NOT NULL,
  city VARCHAR(100) NOT NULL,
  state_province VARCHAR(100),
  postal_code VARCHAR(20) NOT NULL,
  country_code CHAR(2) NOT NULL, 
  address_type VARCHAR(50),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE addresses REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_addresses
BEFORE UPDATE ON addresses
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

ALTER TABLE users
ADD CONSTRAINT fk_default_shipping_address FOREIGN KEY (default_shipping_address_id) REFERENCES addresses(address_id) ON DELETE SET NULL,
ADD CONSTRAINT fk_default_billing_address FOREIGN KEY (default_billing_address_id) REFERENCES addresses(address_id) ON DELETE SET NULL;

CREATE TABLE sellers (
  seller_id SERIAL PRIMARY KEY,
  user_id INTEGER UNIQUE NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  company_name VARCHAR(255) NOT NULL,
  tax_id VARCHAR(100) UNIQUE,
  registration_date DATE DEFAULT now(),
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE sellers REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_sellers
BEFORE UPDATE ON sellers
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE categories (
  category_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  parent_category_id INTEGER REFERENCES categories(category_id) ON DELETE SET NULL,
  description TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE categories REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_categories
BEFORE UPDATE ON categories
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE products (
  product_id SERIAL PRIMARY KEY,
  seller_id INTEGER NOT NULL REFERENCES sellers(seller_id) ON DELETE CASCADE,
  category_id INTEGER NOT NULL REFERENCES categories(category_id) ON DELETE RESTRICT,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  sku VARCHAR(100) UNIQUE NOT NULL,
  base_price DECIMAL(10, 2) NOT NULL CHECK (base_price >= 0), 
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_products
BEFORE UPDATE ON products
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE inventory_pg (
  inventory_id SERIAL PRIMARY KEY,
  product_id INTEGER UNIQUE NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
  quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0), 
  reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
  last_restock_date TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE inventory_pg REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_inventory_pg
BEFORE UPDATE ON inventory_pg
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE RESTRICT,
  order_date TIMESTAMPTZ DEFAULT now() NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'pending', 
  total_amount DECIMAL(12, 2) NOT NULL CHECK (total_amount >= 0), 
  shipping_address_id INTEGER NOT NULL REFERENCES addresses(address_id) ON DELETE RESTRICT,
  billing_address_id INTEGER NOT NULL REFERENCES addresses(address_id) ON DELETE RESTRICT,
  shipping_method VARCHAR(100),
  tracking_number VARCHAR(100),
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_orders
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE order_items (
  order_item_id SERIAL PRIMARY KEY,
  order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
  product_id INTEGER NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0), 
  subtotal DECIMAL(12, 2) NOT NULL, 
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (order_id, product_id)
);
ALTER TABLE order_items REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_order_items
BEFORE UPDATE ON order_items
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE payments (
  payment_id SERIAL PRIMARY KEY,
  order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE RESTRICT,
  payment_date TIMESTAMPTZ DEFAULT now() NOT NULL,
  payment_method VARCHAR(50) NOT NULL, 
  transaction_id VARCHAR(255) UNIQUE,
  amount DECIMAL(12, 2) NOT NULL CHECK (amount >= 0),
  status VARCHAR(50) NOT NULL DEFAULT 'pending', 
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE payments REPLICA IDENTITY FULL;

CREATE TRIGGER set_timestamp_payments
BEFORE UPDATE ON payments
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();


CREATE INDEX idx_users_default_shipping_address_id ON users(default_shipping_address_id);
CREATE INDEX idx_users_default_billing_address_id ON users(default_billing_address_id);
CREATE INDEX idx_users_email ON users(email); 
CREATE INDEX idx_users_username ON users(username); 


CREATE INDEX idx_addresses_user_id ON addresses(user_id);
CREATE INDEX idx_addresses_country_code ON addresses(country_code);
CREATE INDEX idx_addresses_postal_code ON addresses(postal_code); 

CREATE INDEX idx_categories_parent_category_id ON categories(parent_category_id);
CREATE INDEX idx_categories_name ON categories(name); 

CREATE INDEX idx_products_seller_id ON products(seller_id);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_sku ON products(sku); 


CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_shipping_address_id ON orders(shipping_address_id);
CREATE INDEX idx_orders_billing_address_id ON orders(billing_address_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_order_date ON orders(order_date);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);

CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_payments_status ON payments(status);
CREATE INDEX idx_payments_transaction_id ON payments(transaction_id); 


INSERT INTO users (username, email, password_hash, first_name, last_name, date_of_birth, phone_number) VALUES
('test_user_01', 'testuser01@example.com', 'test_password_hash', 'Test', 'User', '1992-01-01', '+1555000111')
RETURNING user_id, currval(pg_get_serial_sequence('users', 'user_id')) AS last_user_id; 

INSERT INTO addresses (user_id, street_address, city, state_province, postal_code, country_code, address_type) VALUES
(currval(pg_get_serial_sequence('users', 'user_id')), '1 Test St', 'Testville', 'TS', '12345', 'US', 'home')
RETURNING address_id, currval(pg_get_serial_sequence('addresses', 'address_id')) AS last_address_id;

UPDATE users 
SET default_shipping_address_id = currval(pg_get_serial_sequence('addresses', 'address_id')), 
    default_billing_address_id = currval(pg_get_serial_sequence('addresses', 'address_id'))
WHERE user_id = currval(pg_get_serial_sequence('users', 'user_id'));

INSERT INTO sellers (user_id, company_name, tax_id) VALUES
(currval(pg_get_serial_sequence('users', 'user_id')), 'Test Seller Co.', 'TESTTAXID01')
RETURNING seller_id, currval(pg_get_serial_sequence('sellers', 'seller_id')) AS last_seller_id;

INSERT INTO categories (name, description) VALUES
('Test Category', 'A category for testing purposes')
RETURNING category_id, currval(pg_get_serial_sequence('categories', 'category_id')) AS last_category_id;

INSERT INTO products (seller_id, category_id, name, description, sku, base_price) VALUES
(currval(pg_get_serial_sequence('sellers', 'seller_id')), currval(pg_get_serial_sequence('categories', 'category_id')), 'Test Product Alpha', 'A test product description.', 'TEST-ALPHA-001', 29.99)
RETURNING product_id, currval(pg_get_serial_sequence('products', 'product_id')) AS last_product_id;

INSERT INTO inventory_pg (product_id, quantity, reserved_quantity, last_restock_date) VALUES
(currval(pg_get_serial_sequence('products', 'product_id')), 50, 1, NOW() - INTERVAL '1 day');

INSERT INTO orders (user_id, status, total_amount, shipping_address_id, billing_address_id, shipping_method) VALUES
(currval(pg_get_serial_sequence('users', 'user_id')), 'pending', 29.99, currval(pg_get_serial_sequence('addresses', 'address_id')), currval(pg_get_serial_sequence('addresses', 'address_id')), 'Test Shipping')
RETURNING order_id, currval(pg_get_serial_sequence('orders', 'order_id')) AS last_order_id;

INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
(currval(pg_get_serial_sequence('orders', 'order_id')), currval(pg_get_serial_sequence('products', 'product_id')), 1, 29.99, 29.99);

INSERT INTO payments (order_id, payment_method, transaction_id, amount, status) VALUES
(currval(pg_get_serial_sequence('orders', 'order_id')), 'test_method', 'test_txn_001', 29.99, 'pending');

