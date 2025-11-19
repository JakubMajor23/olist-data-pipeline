
COPY olist_customers_dataset FROM '/data-csv/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_sellers_dataset FROM '/data-csv/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_products_dataset FROM '/data-csv/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_orders_dataset FROM '/data-csv/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_order_items_dataset FROM '/data-csv/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_order_payments_dataset FROM '/data-csv/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_order_reviews_dataset FROM '/data-csv/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

COPY olist_geolocation_dataset FROM '/data-csv/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;


ALTER TABLE olist_order_items_dataset
ADD COLUMN order_purchase_timestamp TIMESTAMP;


ALTER TABLE olist_order_payments_dataset
ADD COLUMN order_purchase_timestamp TIMESTAMP;

UPDATE olist_order_items_dataset items
SET order_purchase_timestamp = orders.order_purchase_timestamp
FROM olist_orders_dataset orders
WHERE items.order_id = orders.order_id;

UPDATE olist_order_payments_dataset payments
SET order_purchase_timestamp = orders.order_purchase_timestamp
FROM olist_orders_dataset orders
WHERE payments.order_id = orders.order_id;
