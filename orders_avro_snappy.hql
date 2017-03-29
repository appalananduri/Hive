use venkat ;
create table orders_avro_bucket
clustered by (order_id) into 16 buckets
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ("parquet.compress"="SNAPPY" , 
    'avro.schema.url'='/user/venkat/orders.avsc');
exit ;

