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
def event(request):
    if request.method == "POST":

        # 获取表单数据
        event_id = int(request.POST.get('event-id'))
        from_date = request.POST.get('from-date')
        to_date = request.POST.get('to-date')
        feature_id = request.POST.get('feature')
        group_id = request.POST.get('group')


        return HttpResponse((event_id,from_date,to_date,feature_id+group_id))

    return render_to_response('event.html')


if __name__=="__main__":
    feature_result = [[1.0, '2019-02-02'], [1.0, '2019-02-04'], [1.0, '2019-02-03'], [1.0, '2019-02-01']]
    group_result =  [[0, None, '2019-02-03'], [0, None, '2019-02-04'], [0, None, '2019-02-01'], [0, None, '2019-02-02']]