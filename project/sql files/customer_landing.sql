CREATE EXTRERNAL TABLE IF NOT EXISTS 'marcos'.'customer_landing' (
'customername' string,
'email' string,
'phone' string,
'birthday' string,
'serialnumber' string,
'registrationdate' bigint,
'lastupdatedate' bigint
)
ROW FORMAT SERDE 'org.openx.data.jasonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
) 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://marcos-data-lake-house/customer/landing'
TBLPROPERTIES ('classification' = 'json');