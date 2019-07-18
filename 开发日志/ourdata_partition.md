1. 数据生成与上传
> python data_generation.py
>
> hdfs dfs -put data.csv /group7/ourdata
2. 建立无分区表
```
drop table if exists group7.ourdata;
create external table group7.ourdata(
	user_id BIGINT,
	event_id INT,
	time TIMESTAMP,
	day INT,
	event_bucket INT,
	p_utm_source INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as TEXTFILE
location '/group7/ourdata';
```
3. 建立有分区表
```
drop table if exists group7.ourdata_partition;
CREATE TABLE group7.ourdata_partition(
	user_id BIGINT,
	event_id INT,
	time TIMESTAMP,
	p_utm_source INT
)
PARTITIONED BY (day INT,event_bucket INT)
STORED AS  PARQUET
Location '/group7/ourdata_partition';
```
4. 向有分区表中插入数据
```
insert into table ourdata_partition
PARTITION (day,event_bucket)
select user_id, event_id, time, p_utm_source
from group7.ourdata;
```
