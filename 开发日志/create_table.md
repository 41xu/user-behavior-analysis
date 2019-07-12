## event_export & user_export 数据表创建及数据加载

考虑到我们已经有了event_export&user_export的数据文件，因此我们只需要将数据文件上传到hdfs,然后建立一张impala外部表，将外部表的存储位置指向数据文件的位置即可。

## 流程

1. put these dirs to hdfs, such as directory "/data/event_export"

./event_export
./user_export

- 将这两个文件上传至hdfs：/user/impala/data/目录下

![](http://ww1.sinaimg.cn/large/006tNc79ly1g4x37o3m2fj30em01saae.jpg)

- 修改这两个目录的权限

  > su - hdfs
  >
  > hdfs dfs -chmod 777 /user/impala/data/event_export
  >
  > hdfs dfs -chmod 777 /user/impala/data/user_export
  >
  > hdfs dfs -chmod 777 /user/impala/data/

- 将数据文件传至这两个文件夹

  >  hdfs dfs -put a740a2eea330c9d2-4d8587f300000000_43119723_data.0. /user/impala/data/event_export
  >
  > hdfs dfs -put 4e4aa23f4c288f5b-73f53eaa00000000_2090620656_data.0. /user/impala/data/user_export

2. modify these sql files, "location" set to "/data/event_export".

./event_export_create_table.sql
./user_export_create_table.sql

- 将LOCATION 改为数据文件的存放位置，然后执行

  > CREATE TABLE rawdata.user_export (
  >   id BIGINT,
  >   first_id STRING,
  >
  >   ...
  > )
  > STORED AS TEXTFILE
  > LOCATION '/user/impala/data/user_export'

3. run the create table sql in hive or impala.
4. query the data:

select * from event_export limit 10;

![](http://ww3.sinaimg.cn/large/006tNc79ly1g4x3e3yhslj313y0hxn3u.jpg)

5. metadata files:

./event_define.csv    event_id to name/cname
./event_property.csv  property column name to property name/cname (event table)
./user_property.csv   property column name to property name/cname (user table)