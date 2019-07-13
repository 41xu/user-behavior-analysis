from django.http import HttpResponse
from pyecharts import Funnel
from django.shortcuts import render_to_response
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
import pandas as pd
import os

REMOTE_HOST = 'http://chfw.github.io/jupyter-echarts/echarts'
# 过程id 和 过程名的对应关系
event_data = pd.read_csv('./sample_data/event_define.csv')
event_map = {}
for i in range(len(event_data)):
    event_map[event_data.iloc[i,0]] = event_data.iloc[i,2]


@csrf_exempt
def remain(request):
    if request.method == "POST":

        # 获取表单数据
        from_date = request.POST.get('from-date')
        to_date = request.POST.get('to-date')
        init_event_id = int(request.POST.get('init-event-id'))
        remain_event_id = int(request.POST.get('remain-event-id'))


        return HttpResponse((from_date,to_date,init_event_id,remain_event_id))

    return render_to_response('remain.html')

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