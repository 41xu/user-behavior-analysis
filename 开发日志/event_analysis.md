```sql
## 一个菜鸡写的事件函数
def remain2(from_time,to_time,event_init,event_remain):
    from_time += " 00:00:00"
    to_time += " 00:00:00"
    from_time = time.strptime(from_time, "%Y-%m-%d %H:%M:%S")
    from_day = str(int(time.mktime(from_time) // 86400))
    to_time = time.strptime(to_time, "%Y-%m-%d %H:%M:%S")
    to_day = str(int(time.mktime(to_time) // 86400))

    cur.execute("use rawdata")
    create_string = "with user_init_event " \
                    "as (select user_id, day as init_day " \
                    "from event_export_partition_parquet_g7 " \
                    "where event_id = "+ event_init +" and day >= "+from_day+" and day <= "+to_day+" )," \
                    "user_cohort as( " \
                    "select e.user_id,i.init_day,(e.day-i.init_day) as cohort_day " \
                    "from event_export_partition_parquet_g7 e LEFT JOIN user_init_event i on e.user_id = i.user_id " \
                    "where e.event_id = "+ event_remain+ " and (e.day-i.init_day)<7 and (e.day-i.init_day)>=0 " \
                    "group by user_id,cohort_day,i.init_day)" \
                    "select count(*),cohort_day,init_day from user_cohort group by init_day,cohort_day order by init_day,cohort_day"

    start = datetime.datetime.now()
    cur.execute(create_string)
    res = cur.fetchall()
    end = datetime.datetime.now()

    print(res)
    print(end - start)
```

