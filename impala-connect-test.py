from impala.dbapi import connect
import datetime
import time


def random_sample(percent):  # 抽样总表
    create_string = "create table random_sample as select * from event_export where user_id%" + str(percent) + "=1"
    cur = conn.cursor()
    cur.execute('use rawdata')
    cur.execute('drop table if exists rawdata.random_sample')
    cur.execute(create_string)


def funnel(event_ids, quary):  # event_ids->tuple; quary->[year,month] # 按月份进行漏斗查询
    # quary处理
    from_month = "'" + quary[0] + "-" + quary[1] + "-01 00:00:0.000000000'"
    if int(quary[1]) < 12:
        to_month = "'" + quary[0] + "-" + "{:0>2d}".format(int(quary[1]) + 1) + "-01 00:00:00.000000000'"
    else:
        to_month = "'" + str(int(quary[0]) + 1) + "-01-01 00:00:00.000000000'"

    count0 = count1 = count2 = count3 = 0  # count默认为0

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


def remain(from_time, to_time, event_init, event_remain):  # from_time: "2019-01-01", event_init: str event_id
    from_time = "'" + from_time + " 00:00:00.000000000'"
    to_time = "'" + to_time + " 00:00:00.000000000'"

    create_string = "create view sample_remain as select event_id,user_id,day,time  from  event_export where event_id in " + \
                   "(" + event_init + "," + event_remain + ")" + " and " + from_time + " <time and time< " + to_time
    cur.execute("use rawdata")
    cur.execute("drop view if exists rawdata.sample_remain")
    cur.execute(create_string)

    counts=[0 for _ in range(7)]
    create_string="select count(distinct user_id),day from sample_remain group by day where event_id="+event_init
    cur.execute(create_string)
    total=cur.fetchall()
    total=[list(x) for x in total]
    for x in total:
        x[1] = str(datetime.datetime.fromtimestamp(x[1] * 86400))[:10]

    create_string="select count(distinct t1.user_id) from sample_remain group by day where event_id="+event_init+" and "+\
        from_time+" < time"




def event(from_time, to_time, event_id, feature,
          group):  # from_time: "2019-01-01", event_id: str, feature: str, group: str
    features = {
        "0": "",  # 总次数
        "1": "",  # 总人数
        "2": "",  # 去重人数
        "3": "",  # 人均次数
        # "4":"", # 平均事件时长
    }
    groups = {
        "0": "",  # 广告系列来源分组
        "1": "",  # 是否首次访问分组
    }
    from_time = "'" + from_time + " 00:00:00.000000000'"
    to_time = "'" + to_time + " 00:00:00.000000000'"

    create_string = "create view sample_event as select * from event_export where event_id=" + event_id + " and " + \
                    from_time + " <time and time< " + to_time

    cur.execute('use rawdata')
    cur.execute('drop view if exists rawdata.sample_event')
    cur.execute(create_string)

    features["0"] = "select count(time),day from sample_event group by day order by day"
    features["1"] = "select count(user_id),day from sample_event group by day order by day"
    features["2"] = "select count(distinct user_id),day from sample_event group by day order by day"
    features["3"] = "select count(time)/count(distinct user_id),day from sample_event group by day order by day"
    # features["4"]="select sum(p__event_duration)/count(p__event_duration),day from sample_event group by day"

    f={
        "0":"count(time)",
        "1":"count(user_id)",
        "2":"count(distinct user_id)",
        "3":"count(time)/count(distinct user_id)"
    }

    # groups["0"] = "select "+f[feature]+",p_utm_source,day from sample_event group by day,p_utm_source order by day"
    # groups["1"] = "select "+f[feature]+",p_is_first_time,day from sample_event group by day,p_is_first_time order by day"
    groups["0"] = "select "+f[feature]+",p__carrier,day from sample_event group by day,p__carrier order by day"
    groups["1"] = "select "+f[feature]+",p__manufacturer,day from sample_event group by day,p__manufacturer order by day"

    cur.execute(features[feature])
    feature_result = cur.fetchall()
    feature_result = [list(x) for x in feature_result]
    for x in feature_result:
        x[1] = str(datetime.datetime.fromtimestamp(x[1] * 86400))[:10]

    cur.execute(groups[group])
    group_result = cur.fetchall()
    group_result = [list(x) for x in group_result]
    for x in group_result:
        x[2] = str(datetime.datetime.fromtimestamp(x[2] * 86400))[:10]

    # 用户选择的event_id可能在限制时间内为None，在演示时为了避免老师认为我们的功能写的是错的，可以在impala中执行这句话
    # select count(p_is_first_time),p_is_first_time,day from event_export where '2019-02-01 00:00:00.000000000'<time
    # and time<'2019-02-07 00:00:00.000000000' group by day,p_is_first_time;

    return feature_result, group_result




# 这里想要处理那些feature_result中没有出现的日期，用0补上
def feature_standard(from_time, to_time, feature_result):  # from_time: str
    from_time += " 00:00:00.000000000"
    to_time += " 00:00:00.000000000"
    from_time = time.strftime(from_time, "%Y-%m-%d %H:%M:%S")
    from_time = time.mktime(from_time)
    to_time = time.strftime(to_time, "%Y-%m-%d %H:%M:%S")
    to_time = time.mktime(to_time)

    from_day = time.mktime(from_time.timetuple()) // 86400
    to_day = time.mktime(to_time.timetuple()) // 86400
    results = []
    for day in range(from_day, to_day + 1):
        results.append([0, day])
    for x in results:
        x[1] = str(datetime.datetime.fromtimestamp(x[1] * 86400))[:10]
    dic = {x[1]: x[0] for x in results}
    for x in feature_result:
        dic[x[1]] = x[0]
    return dic


def group_standard(from_time, to_time, group_result):  # from_time: unixtime
    from_time += " 00:00:00.000000000"
    to_time += " 00:00:00.000000000"
    from_day = time.mktime(from_time.timetuple()) // 86400
    to_day = time.mktime(to_time.timetuple()) // 86400
    results = []
    for day in range(from_day, to_day + 1):
        results.append([0, day])
    for x in results:
        x[2] = str(datetime.datetime.fromtimestamp(x[2] * 86400))[:10]
    dic = {x[2]: [x[0], x[1]] for x in results}
    for x in group_result:
        dic[x[2]] = [x[0], [x[1]]]
    return dic



if __name__ == '__main__':
    conn = connect(host='139.217.87.136', port=21050)
    cur = conn.cursor()
    cur.execute('use rawdata')
    cur.execute('select count(*) from event_export_partition')
    print(cur.fetchall())
    # event_ids = (5, 19, 28, 1)
    # quary = ["2019", "02"]
    # count=funnel(event_ids, quary)
    # print(count)
    # from_time = "2019-01-01"
    # to_time = "2019-03-01"
    # event_id = "28"
    # feature = "0"
    # group = "1"
    # features,groups=event(from_time,to_time,event_id,feature,group)
    # print(features)
    # print(groups)
    # event_init="26" # 注册
    # event_remain="27" # 完成项目创建
    # remain(from_time,to_time,event_init,event_remain)


# select * from(select count(time), day from event_export group by day) f left join (select count(time),p_is_first_time,day from event_export group by day,p_is_first_time) g on f.day=g.day;
# feature0,group0

