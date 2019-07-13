from impala.dbapi import connect
import datetime


def random_sample(percent):  # 抽样总表
    create_string = "create table random_sample as select * from event_export where user_id%" + str(percent) + "=1"
    cur = conn.cursor()
    cur.execute('use rawdata')
    cur.execute('drop table if exists rawdata.random_sample')
    cur.execute(create_string)


def funnel(event_ids, quary):  # event_ids->tuple; quary->[year,month]
    # quary处理
    from_month = "'" + quary[0] + "-" + quary[1] + "-01 00:00:0.000000000'"
    if int(quary[1]) < 12:
        to_month = "'" + quary[0] + "-" + "{:0>2d}".format(int(quary[1]) + 1) + "-01 00:00:00.000000000'"
    else:
        to_month = "'" + str(int(quary[0]) + 1) + "-01-01 00:00:00.000000000'"

    count0=count1=count2=count3=0 # count默认为0

    # 抽取只含查询状态的数据

    # 使用抽样数据演示
    # random_sample(200)
    # create_string = "create view sample_funnel as select user_id, event_id, time from random_sample where event_id in" + \
    #                 str(event_ids) + " and " + from_month + " <time and time< " + to_month

    # 总表测试
    create_string = "create view sample_funnel as select user_id, event_id, time from event_export where event_id in" + \
                    str(event_ids) + " and " + from_month + " <time and time< " + to_month
    cur.execute('use rawdata')
    cur.execute('drop view if exists rawdata.sample_funnel')
    cur.execute(create_string)
    cur.execute('select count(time) from sample_funnel where event_id=' + str(event_ids[0]))
    count0 = cur.fetchall()[0][0]
    create_string = "select count(t1.time),count(t2.time), count(t3.time) from (select * from sample_funnel where event_id=" \
                    + str(event_ids[0]) + ") t0" + \
                    " left join (select * from sample_funnel where event_id=" + str(event_ids[1]) + ") t1" + \
                    " on t0.user_id=t1.user_id and t0.time<t1.time and timestamp_cmp(t0.time + interval 120 minutes, t1.time)=1" + \
                    " left join (select * from sample_funnel where event_id=" + str(event_ids[2]) + ") t2" + \
                    " on t1.user_id=t2.user_id and t1.time<t2.time and timestamp_cmp(t1.time + interval 120 minutes, t2.time)=1" + \
                    " left join (select * from sample_funnel where event_id=" + str(event_ids[3]) + ") t3" + \
                    " on t2.user_id=t3.user_id and t2.time<t3.time and timestamp_cmp(t2.time + interval 120 minutes, t3.time)=1"
    cur.execute(create_string)
    data = cur.fetchall()
    count1, count2, count3 = data[0][0], data[0][1], data[0][2]
    return count0, count1, count2, count3


def event(from_time,to_time,event_id,feature,group): # from_time: "2019-01-01", event_id: str, feature: str, group: str
    features={
        "0":"", # 总次数
        "1":"", # 总人数
        "2":"", # 去重人数
        "3":"", # 人均次数
        # "4":"", # 平均事件时长
    }
    groups={
        "0":"", # 广告系列来源分组
        "1":"", # 是否首次访问分组
    }
    from_time="'"+from_time+" 00:00:00.000000000'"
    to_time="'"+to_time+" 00:00:00.000000000'"

    create_string="create view sample_event as select * from event_export where event_id="+event_id+" and "+\
        from_time+" <time and time< "+to_time

    cur.execute('use rawdata')
    cur.execute('drop view if exists rawdata.sample_event')
    cur.execute(create_string)

    features["0"]="select count(time),day from sample_event group by day"
    features["1"]="select count(user_id),day from sample_event group by day"
    features["2"]="select count(distinct user_id),day from sample_event group by day "
    features["3"]="select count(time)/count(distinct user_id),day from sample_event group by day"
    # features["4"]="select sum(p__event_duration)/count(p__event_duration),day from sample_event group by day"

    groups["0"]="select count(p_utm_source),p_utm_source,day from sample_event group by day,p_utm_source"
    groups["1"]="select count(p_is_first_time),p_is_first_time,day from sample_event group by day,p_is_first_time"


    cur.execute(features[feature])
    feature_result=cur.fetchall()
    feature_result=[list(x) for x in feature_result]
    for x in feature_result:
        x[1]=str(datetime.datetime.fromtimestamp(x[1]*86400))[:10]

    cur.execute(groups[group])
    group_result=cur.fetchall()
    group_result=[list(x) for x in group_result]
    for x in group_result:
        x[2]=str(datetime.datetime.fromtimestamp(x[2]*86400))[:10]

    # 用户选择的event_id可能在限制时间内为None，在演示时为了避免老师认为我们的功能写的是错的，可以在impala中执行这句话
    # select count(p_is_first_time),p_is_first_time,day from event_export where '2019-02-01 00:00:00.000000000'<time
    # and time<'2019-02-07 00:00:00.000000000' group by day,p_is_first_time;

    return feature_result,group_result


if __name__ == '__main__':
    conn = connect(host='139.217.87.136', port=21050)
    cur = conn.cursor()
    event_ids = (5, 19, 28, 1)
    quary = ["2019", "02"]
    # count=funnel(event_ids, quary)
    # print(count)
    from_time="2019-02-01"
    to_time="2019-02-05"
    event_id="28"
    feature="3"
    group="1"
    features,groups=event(from_time,to_time,event_id,feature,group)
    print(features,groups)
