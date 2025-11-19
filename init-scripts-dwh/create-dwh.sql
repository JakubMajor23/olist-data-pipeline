CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS dwh_main;
CREATE SCHEMA IF NOT EXISTS dwh_main_dev;


CREATE TABLE IF NOT EXISTS raw_data.olist_products_dataset (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE IF NOT EXISTS raw_data.olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city VARCHAR(50),
    geolocation_state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS raw_data.olist_sellers_dataset (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10) NOT NULL,
    seller_city VARCHAR(50) NOT NULL,
    seller_state VARCHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_data.olist_customers_dataset (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NOT NULL,
    customer_city VARCHAR(50) NOT NULL,
    customer_state VARCHAR(2) NOT NULL
);


CREATE TABLE IF NOT EXISTS raw_data.olist_orders_dataset (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_data.olist_order_payments_dataset (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(20) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DOUBLE PRECISION NOT NULL,
    order_purchase_timestamp TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);


CREATE TABLE IF NOT EXISTS raw_data.olist_order_items_dataset (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    freight_value DOUBLE PRECISION NOT NULL,
    order_purchase_timestamp TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS raw_data.olist_order_reviews_dataset (
    review_id VARCHAR(50),
    order_id VARCHAR(50) NOT NULL,
    review_score INT NOT NULL,
    review_comment_title VARCHAR(150),
    review_comment_message VARCHAR(500),
    review_creation_date DATE NOT NULL,
    review_answer_timestamp TIMESTAMP
);