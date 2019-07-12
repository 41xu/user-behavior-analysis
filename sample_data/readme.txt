some example data for testing.
by zhangtianxiao@sensorsdata.cn

how to use this?

1. put these dirs to hdfs, such as directory "/data/event_export"

./event_export
./user_export

2. modify these sql files, "location" set to "/data/event_export".

./event_export_create_table.sql
./user_export_create_table.sql

3. run the create table sql in hive or impala.

4. query the data:

select * from event_export limit 10;

5. metadata files:

./event_define.csv    event_id to name/cname
./event_property.csv  property column name to property name/cname (event table)
./user_property.csv   property column name to property name/cname (user table)
