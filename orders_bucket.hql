use venkat ;
CREATE TABLE orders_bucket (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
CLUSTERED BY (order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
