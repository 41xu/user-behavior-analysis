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
        # value = function(event_ids,query)

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