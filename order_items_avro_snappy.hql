use venkat ;
create table order_items_avro_bucket
clustered by (order_item_order_id) into 16 buckets
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ("parquet.compress"="SNAPPY" , 
    'avro.schema.url'='/user/venkat/order_items.avsc');
exit ;

