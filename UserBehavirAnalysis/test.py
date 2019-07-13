from impala.dbapi import connect

conn = connect(host='139.217.87.136', port=21050)
cur = conn.cursor()
cur.execute('use rawdata;')
cur.execute('select user_id from sample limit 100 ;')
data_list=cur.fetchall()
for row in data_list:
    print(row)

'''
select event_id ,user_id ,time from sample where user_id =5472601952262823501 and event_id in (5,19,28,1);


select session_id,req_url,req_time from(
    select session_id,req_url,req_time,rank() over(partition by session_id,req_url order by req_time desc) as rank 
    from(
        select session_id,req_url,req_time
        from t_visitlog
        distribute by session_id,req_url
        sort by session_id,req_time desc
    )a
)b
where rank = 1;

= str(int(month)+1) if month > 10 or '0'+ str(int(month)+1)

'''
