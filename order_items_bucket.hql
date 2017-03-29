use venkat ;
CREATE TABLE order_items_bucket (
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
CLUSTERED BY (order_item_order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
