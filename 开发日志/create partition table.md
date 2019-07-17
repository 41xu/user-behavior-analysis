### 1. 建立无分区表

```sql
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
location '/group7/ourdata'
```



### 2. 建立 有分区表

```sql
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

### 3. 向无分区表中插入数据

```sql
insert into table ourdata_partition
PARTITION(day,event_bucket) 
select user_id,
	event_id,
	time,
	p_utm_source,
	day,
	event_bucket
from group7.ourdata;
```

