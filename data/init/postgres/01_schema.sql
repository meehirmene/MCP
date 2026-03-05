-- FlowForge E-Commerce Schema for PostgreSQL
-- This creates the source tables that our CDC pipeline will stream from

CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    inventory_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10, 2) NOT NULL,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create publication for CDC (logical replication)
CREATE PUBLICATION flowforge_pub FOR ALL TABLES;

-- Insert sample data
INSERT INTO customers (email, first_name, last_name, region) VALUES
    ('alice@example.com', 'Alice', 'Johnson', 'us-east'),
    ('bob@example.com', 'Bob', 'Smith', 'us-west'),
    ('carol@example.com', 'Carol', 'Williams', 'eu-west'),
    ('dave@example.com', 'Dave', 'Brown', 'eu-east'),
    ('eve@example.com', 'Eve', 'Davis', 'ap-south');

INSERT INTO products (name, category, price, inventory_count) VALUES
    ('Wireless Headphones', 'Electronics', 79.99, 500),
    ('Running Shoes', 'Sports', 129.99, 250),
    ('Coffee Maker', 'Kitchen', 49.99, 300),
    ('Laptop Stand', 'Electronics', 39.99, 400),
    ('Yoga Mat', 'Sports', 24.99, 600),
    ('Water Bottle', 'Sports', 14.99, 1000),
    ('Bluetooth Speaker', 'Electronics', 59.99, 350),
    ('Desk Lamp', 'Home', 34.99, 200);

INSERT INTO orders (customer_id, status, total_amount, region) VALUES
    (1, 'completed', 159.98, 'us-east'),
    (2, 'completed', 129.99, 'us-west'),
    (3, 'processing', 89.98, 'eu-west'),
    (4, 'shipped', 39.99, 'eu-east'),
    (5, 'pending', 49.99, 'ap-south');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 79.99),
    (1, 4, 2, 39.99),
    (2, 2, 1, 129.99),
    (3, 3, 1, 49.99),
    (3, 4, 1, 39.99),
    (4, 4, 1, 39.99),
    (5, 3, 1, 49.99);

INSERT INTO payments (order_id, amount, payment_method, status) VALUES
    (1, 159.98, 'credit_card', 'completed'),
    (2, 129.99, 'paypal', 'completed'),
    (3, 89.98, 'credit_card', 'pending'),
    (4, 39.99, 'debit_card', 'completed'),
    (5, 49.99, 'credit_card', 'pending');
