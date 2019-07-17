from django.http import HttpResponse
from pyecharts import Funnel
from django.shortcuts import render_to_response
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
import pandas as pd
import os
from impala.dbapi import connect
import datetime
import time

REMOTE_HOST = 'http://chfw.github.io/jupyter-echarts/echarts'
# 过程id 和 过程名的对应关系
event_data = pd.read_csv('./sample_data/event_define.csv')
event_map = {}
for i in range(len(event_data)):
    event_map[event_data.iloc[i,0]] = event_data.iloc[i,2]


@csrf_exempt
def funnel(request):
    if request.method == "POST":

        # 获取表单数据
        event_id_1 = int(request.POST.get('event-id-1'))
        event_id_2 = int(request.POST.get('event-id-2'))
        event_id_3 = int(request.POST.get('event-id-3'))
        event_id_4 = int(request.POST.get('event-id-4'))
        year = request.POST.get('year')
        month = request.POST.get('month')
        # 将表单数据格式化
        event_ids = [event_id_1, event_id_2, event_id_3, event_id_4]
        query = year+month

        # 从impala获取结果
        value = function(event_ids,query)

        event_names = [event_map[id] for id in event_ids]# 将envent_id 转换成 event_names
        template = loader.get_template('funnel.html')
        # funnel_diagram = funnel_generate(event_names,value)
        funnel_diagram = funnel_generate2()
        context = dict(
            host=REMOTE_HOST,
            funnel=funnel_diagram.render_embed(),
            script_list_funnel=funnel_diagram.get_js_dependencies()
        )
        return HttpResponse(template.render(context, request))
    return render_to_response('funnel.html')

def function(event_ids, quary):  # event_ids->tuple; quary->[year,month] # 按月份进行漏斗查询
    conn = connect(host='xxx.xxx.xxx.xxx', port=21050)
    cur = conn.cursor()
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
    create_string = "create view sample_funnel as select user_id, event_id, time from event_export_partition where event_id in" + \
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


def funnel_generate2():
    attr = ["过程1", "过程2", "过3", "过程4"]
    value = [80, 60, 40, 20]
    funnel = Funnel("漏斗图")
    funnel.add(
        " ",
        attr,
        value,
        is_label_show=True,
        label_pos="inside",
        label_text_color="#fff",
    )
    return funnel
def funnel_generate(event_names,values):
    # process -> list 用户选择的漏斗过程 ； values -> list impala返回的结果
    # attr = ["过程1", "过程2", "过3", "过程4"]
    attr = event_names
    # value = [80, 60, 40, 20]
    value = values
    funnel = Funnel("漏斗图")
    funnel.add(
        " ",
        attr,
        value,
        is_label_show=True,
        label_pos="inside",
        label_text_color="#fff",
    )
    return funnel

if __name__=="__main__":
   pass