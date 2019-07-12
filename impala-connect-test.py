from impala.dbapi import connect
import time


# conn = connect(host='139.217.87.136', port=21050)
# cur = conn.cursor()
# cur.execute('use rawdata')

# start=time.time()
# print("start")
# cur.execute('select * from event_export where event_id=1')
# data=cur.fetchall()
# end=time.time()
# a=end-start
# print("time:",a)
# for x in data:
#     print(x)

def random_sample(percent):
    # 抽样总表
    create_string="create table random_sample as select * from event_export where user_id%"+str(percent)+"=1"
    cur=conn.cursor()
    cur.execute('use rawdata')
    cur.execute('drop table if exists rawdata.random_sample')
    cur.execute(create_string)

def sample(event_ids,quary): # event_ids->tuple; quary->[year,month]
    # quary处理
    from_month = "'"+quary[0] + "-" + quary[1] + "-01 00:00:0.000000000'"
    if int(quary[1])<12:
        to_month="'"+quary[0]+"-"+"{:0>2d}".format(int(quary[1])+1)+"-01 00:00:00.000000000'"
    else:
        to_month="'"+str(int(quary[0])+1)+"-01-01 00:00:00.000000000'"

    # 抽取只含查询状态的数据
    # 使用抽样数据演示

    # random_sample(400)
    create_string="create view sample_funnel as select user_id, event_id, time from random_sample where event_id in"+\
                  str(event_ids)+" and "+from_month+" <time and time< "+to_month
    cur.execute('use rawdata')
    cur.execute('drop view if exists rawdata.sample_funnel')
    cur.execute(create_string)
    cur.execute('select count(time) from sample_funnel where event_id='+str(event_ids[0]))
    count0=cur.fetchall()[0][0]
    create_string="select count(t1.time) from (select * from sample_funnel where event_id="+str(event_ids[0])+") t0"+\
        " left join (select * from sample_funnel where event_id="+str(event_ids[1])+") t1"+\
        " on t0.user_id=t1.user_id and t0.time<t1.time and timestamp_cmp(t0.time + interval 120 minutes, t1.time)=1"
    cur.execute(create_string)
    count1=cur.fetchall()[0][0]
    print(count0,count1)
    # cur.execute('select count(user_id) from '
    #             '(select * from sample_funnel where time>'+str(quary_month))'
    # print(data)
    # cur.execute('drop table if exists sample')
    # cur.execute('create table sample as select * from event_export where user_id%500 =1 ')
    # cur.execute('select event_id, user_id, time from (select event_id, user_id, time from sample where event_id in event_ids order by user_id,time ')
    #
    # data=cur.fetchall()


if __name__=='__main__':
    conn=connect(host='139.217.87.136', port=21050)
    cur=conn.cursor()
    event_ids=(5,19,28,1)
    quary=["2019","02"]
    sample(event_ids,quary)