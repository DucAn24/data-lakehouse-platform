DROP TABLE IF EXISTS order_reviews CASCADE;
DROP TABLE IF EXISTS order_payments CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS sellers CASCADE;
DROP TABLE IF EXISTS geolocation CASCADE;
DROP TABLE IF EXISTS product_category_name_translation CASCADE;

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100) NOT NULL
);

CREATE TABLE customers (
    customer_id VARCHAR(100) PRIMARY KEY,
    customer_unique_id VARCHAR(100) NOT NULL,
    customer_zip_code_prefix VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2)
);

-- CREATE INDEX idx_customers_unique_id ON customers(customer_unique_id);
-- CREATE INDEX idx_customers_state ON customers(customer_state);

CREATE TABLE sellers (
    seller_id VARCHAR(100) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(50),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

-- CREATE INDEX idx_sellers_state ON sellers(seller_state);

CREATE TABLE products (
    product_id VARCHAR(100) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght VARCHAR(50),
    product_description_lenght VARCHAR(50),
    product_photos_qty VARCHAR(50),
    product_weight_g VARCHAR(50),
    product_length_cm VARCHAR(50),
    product_height_cm VARCHAR(50),
    product_width_cm VARCHAR(50)
    -- FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation(product_category_name)
);

-- CREATE INDEX idx_products_category ON products(product_category_name);

CREATE TABLE orders (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
    -- FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- CREATE INDEX idx_orders_customer_id ON orders(customer_id);
-- CREATE INDEX idx_orders_status ON orders(order_status);
-- CREATE INDEX idx_orders_purchase_timestamp ON orders(order_purchase_timestamp);

CREATE TABLE order_items (
    order_id VARCHAR(100) NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    seller_id VARCHAR(100) NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
    -- FOREIGN KEY (order_id) REFERENCES orders(order_id),
    -- FOREIGN KEY (product_id) REFERENCES products(product_id)
    -- FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

-- CREATE INDEX idx_order_items_product_id ON order_items(product_id);
-- CREATE INDEX idx_order_items_seller_id ON order_items(seller_id);

CREATE TABLE order_payments (
    order_id VARCHAR(100) NOT NULL,
    payment_sequential BIGINT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments BIGINT NOT NULL,
    payment_value DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
    -- FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- CREATE INDEX idx_order_payments_type ON order_payments(payment_type);

CREATE TABLE order_reviews (
    review_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    review_score INTEGER NOT NULL CHECK (review_score >= 1 AND review_score <= 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP
    -- FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- CREATE INDEX idx_order_reviews_order_id ON order_reviews(order_id);
-- CREATE INDEX idx_order_reviews_score ON order_reviews(review_score);

CREATE TABLE geolocation (
    geolocation_id SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(50) NOT NULL,
    geolocation_lat VARCHAR(20) NOT NULL,
    geolocation_lng VARCHAR(20) NOT NULL,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(2)
);

-- CREATE INDEX idx_geolocation_zip ON geolocation(geolocation_zip_code_prefix);
-- CREATE INDEX idx_geolocation_state ON geolocation(geolocation_state);
-- CREATE INDEX idx_geolocation_city ON geolocation(geolocation_city);
