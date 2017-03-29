use venkat ;
create external table departments_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location '/tmp/working/departments_avro'
  TBLPROPERTIES (
    'avro.schema.url'='/user/venkat/departments.avsc');
exit ;

