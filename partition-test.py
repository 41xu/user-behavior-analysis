from impala.dbapi import connect
import numpy as np

host = '106.75.95.67'


def get_day_bucket():
    conn = connect(host=host, port=21050)
    cur = conn.cursor()
    cur.execute("use group7")
    create_string = "select day,event_bucket from ourdata group by day,event_bucket"
    cur.execute(create_string)
    result = cur.fetchall()
    file = open('data.txt', 'w')
    file.write(str(result))
    file.close()


if __name__ == '__main__':

    conn = connect(host=host, port=21050)
    cur = conn.cursor()
    cur.execute("use group7")
    create_string = "select day,event_bucket from ourdata group by day,event_bucket"
    cur.execute(create_string)
    result = cur.fetchall()
    print("all day,event_bucket get.")
    count=0
    for x in result:
        create_string = "insert into table ourdata_partition PARTITION (day=" + \
                        str(x[0]) + ",event_bucket=" + str(x[1]) + ") select user_id,event_id,time,day,p_utm_source,event_bucket " + \
                        "from group7.ourdata"
        cur.execute(create_string)
        print(count," has been done.")
        count+=1
