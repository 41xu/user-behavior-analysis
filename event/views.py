from django.http import HttpResponse
from pyecharts import Funnel
from django.shortcuts import render_to_response
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
import pandas as pd
from pyecharts import Line
import os
import datetime
import time
from function import xusy

REMOTE_HOST = 'http://chfw.github.io/jupyter-echarts/echarts'
host = '105.75.95.67'

@csrf_exempt
def event(request):
    if request.method == "POST":

        # 获取表单数据
        event_id = request.POST.get('event-id')
        from_date = request.POST.get('from-date')
        to_date = request.POST.get('to-date')
        feature_id = request.POST.get('feature')
        group_id = request.POST.get('group')

        feature_result,group_result = xusy.event(host,from_date,to_date,event_id,feature_id,group_id)

        template = loader.get_template('event.html')
        event_diagram = generate_chart(from_date,to_date,)
        context = dict(
            host=REMOTE_HOST,
            event=event_diagram.render_embed(),
            script_list_event=event_diagram.get_js_dependencies()
        )
        return HttpResponse(template.render(context, request))

    return render_to_response('event.html')

def time2timestamp(date):
    date += " 00:00:00"
    date = int(time.mktime(time.strptime(date, "%Y-%m-%d %H:%M:%S")))//86400
    return date
def generate_chart(from_date,to_date,feature_result,group_result):
    # from_date = '2019-01-01'
    # to_date = '2019-02-01'
    from_time = time2timestamp(from_date)
    to_time = time2timestamp(to_date)
    days = to_time - from_time - 1

    attr = [str(datetime.datetime.fromtimestamp(date * 86400))[:10] for date in range(from_time + 1, to_time)]
    group_dict = {}
    '''
        {
        'Dell':[, , , , ,],
        'apple:[   ]
        }
    '''
    line = Line("事件分析")
    feature_value = [0] * days
    for li in feature_result:
        feature_value[li[1] - from_time - 1] = li[0]
    line.add('all', attr, feature_value)

    for group in group_result:
        if group[1] not in group_dict:
            group_dict[group[1]] = [0] * days
        group_dict[group[1]][group[2] - from_time - 1] = group[0]
    for key, value in group_dict.items():
        line.add(key, attr, value)

    return line

if __name__=="__main__":
    feature_result = [[124, 17897], [138, 17898], [126, 17899], [132, 17900], [140, 17901], [162, 17902], [138, 17903], [170, 17904], [164, 17905], [154, 17906], [166, 17907], [166, 17908], [124, 17909], [168, 17910], [178, 17911], [164, 17912], [128, 17913], [188, 17914], [184, 17915], [182, 17916], [180, 17917], [168, 17918], [180, 17919], [164, 17920], [164, 17921], [212, 17922], [176, 17923], [160, 17924], [194, 17925], [204, 17926]]
    group_result =  [[2, 'Dell', 17897], [8, 'Sumsung', 17897], [56, 'Apple', 17897], [4, 'ASUS', 17897], [20, 'xiaomi', 17897], [30, 'Huawei', 17897], [2, 'HP', 17897], [2, 'Lenovo', 17897], [16, 'Sumsung', 17898], [4, 'HP', 17898], [6, 'Dell', 17898], [32, 'Huawei', 17898], [68, 'Apple', 17898], [12, 'xiaomi', 17898], [72, 'Apple', 17899], [16, 'xiaomi', 17899], [4, 'HP', 17899], [6, 'ASUS', 17899], [14, 'Sumsung', 17899], [14, 'Huawei', 17899], [2, 'HP', 17900], [8, 'Sumsung', 17900], [4, 'Dell', 17900], [34, 'Huawei', 17900], [16, 'xiaomi', 17900], [2, 'Lenovo', 17900], [62, 'Apple', 17900], [4, 'ASUS', 17900], [64, 'Apple', 17901], [4, 'Dell', 17901], [2, 'HP', 17901], [32, 'Huawei', 17901], [20, 'Sumsung', 17901], [14, 'xiaomi', 17901], [4, 'ASUS', 17901], [28, 'xiaomi', 17902], [2, 'HP', 17902], [4, 'ASUS', 17902], [8, 'Dell', 17902], [14, 'Sumsung', 17902], [38, 'Huawei', 17902], [68, 'Apple', 17902], [14, 'Sumsung', 17903], [22, 'Huawei', 17903], [10, 'ASUS', 17903], [78, 'Apple', 17903], [2, 'Lenovo', 17903], [8, 'xiaomi', 17903], [4, 'Dell', 17903], [24, 'Sumsung', 17904], [2, 'HP', 17904], [2, 'ASUS', 17904], [82, 'Apple', 17904], [6, 'Dell', 17904], [16, 'xiaomi', 17904], [2, 'Lenovo', 17904], [36, 'Huawei', 17904], [18, 'Sumsung', 17905], [70, 'Apple', 17905], [32, 'Huawei', 17905], [2, 'ASUS', 17905], [6, 'HP', 17905], [30, 'xiaomi', 17905], [6, 'Lenovo', 17905], [16, 'xiaomi', 17906], [26, 'Huawei', 17906], [18, 'Sumsung', 17906], [84, 'Apple', 17906], [2, 'Lenovo', 17906], [4, 'Dell', 17906], [4, 'ASUS', 17906], [16, 'xiaomi', 17907], [8, 'Dell', 17907], [18, 'Sumsung', 17907], [36, 'Huawei', 17907], [8, 'ASUS', 17907], [62, 'Apple', 17907], [6, 'Lenovo', 17907], [12, 'HP', 17907], [2, 'Lenovo', 17908], [38, 'Huawei', 17908], [16, 'Sumsung', 17908], [82, 'Apple', 17908], [6, 'ASUS', 17908], [8, 'Dell', 17908], [12, 'xiaomi', 17908], [2, 'HP', 17908], [20, 'Sumsung', 17909], [16, 'xiaomi', 17909], [4, 'HP', 17909], [2, 'Dell', 17909], [32, 'Huawei', 17909], [50, 'Apple', 17909], [84, 'Apple', 17910], [2, 'HP', 17910], [32, 'Huawei', 17910], [10, 'Dell', 17910], [14, 'Sumsung', 17910], [6, 'ASUS', 17910], [16, 'xiaomi', 17910], [4, 'Lenovo', 17910], [30, 'Huawei', 17911], [86, 'Apple', 17911], [20, 'Sumsung', 17911], [10, 'ASUS', 17911], [18, 'xiaomi', 17911], [2, 'HP', 17911], [12, 'Dell', 17911], [16, 'Sumsung', 17912], [62, 'Apple', 17912], [18, 'xiaomi', 17912], [2, 'Dell', 17912], [2, 'Lenovo', 17912], [6, 'ASUS', 17912], [50, 'Huawei', 17912], [8, 'HP', 17912], [2, 'Lenovo', 17913], [4, 'HP', 17913], [4, 'Sumsung', 17913], [14, 'xiaomi', 17913], [74, 'Apple', 17913], [24, 'Huawei', 17913], [4, 'Dell', 17913], [2, 'ASUS', 17913], [4, 'Dell', 17914], [2, 'HP', 17914], [4, 'Lenovo', 17914], [24, 'Sumsung', 17914], [4, 'ASUS', 17914], [72, 'Apple', 17914], [30, 'xiaomi', 17914], [48, 'Huawei', 17914], [4, 'HP', 17915], [16, 'xiaomi', 17915], [6, 'Dell', 17915], [108, 'Apple', 17915], [6, 'ASUS', 17915], [4, 'Lenovo', 17915], [8, 'Sumsung', 17915], [32, 'Huawei', 17915], [40, 'Huawei', 17916], [26, 'xiaomi', 17916], [6, 'Dell', 17916], [88, 'Apple', 17916], [2, 'Lenovo', 17916], [4, 'ASUS', 17916], [2, 'HP', 17916], [14, 'Sumsung', 17916], [14, 'Sumsung', 17917], [34, 'xiaomi', 17917], [2, 'ASUS', 17917], [74, 'Apple', 17917], [6, 'Dell', 17917], [50, 'Huawei', 17917], [42, 'Huawei', 17918], [4, 'ASUS', 17918], [72, 'Apple', 17918], [18, 'xiaomi', 17918], [16, 'Sumsung', 17918], [12, 'Dell', 17918], [4, 'Lenovo', 17918], [6, 'Dell', 17919], [2, 'ASUS', 17919], [58, 'Huawei', 17919], [70, 'Apple', 17919], [22, 'Sumsung', 17919], [22, 'xiaomi', 17919], [72, 'Apple', 17920], [2, 'Lenovo', 17920], [4, 'HP', 17920], [6, 'ASUS', 17920], [36, 'Huawei', 17920], [4, 'Dell', 17920], [16, 'Sumsung', 17920], [24, 'xiaomi', 17920], [12, 'Sumsung', 17921], [2, 'Lenovo', 17921], [60, 'Apple', 17921], [4, 'HP', 17921], [16, 'xiaomi', 17921], [8, 'ASUS', 17921], [6, 'Dell', 17921], [56, 'Huawei', 17921], [2, 'HP', 17922], [24, 'xiaomi', 17922], [18, 'Sumsung', 17922], [6, 'ASUS', 17922], [62, 'Huawei', 17922], [8, 'Dell', 17922], [92, 'Apple', 17922], [4, 'ASUS', 17923], [2, 'Lenovo', 17923], [4, 'HP', 17923], [6, 'Sumsung', 17923], [18, 'xiaomi', 17923], [96, 'Apple', 17923], [6, 'Dell', 17923], [40, 'Huawei', 17923], [12, 'Sumsung', 17924], [76, 'Apple', 17924], [2, 'Lenovo', 17924], [22, 'xiaomi', 17924], [34, 'Huawei', 17924], [4, 'HP', 17924], [6, 'Dell', 17924], [4, 'ASUS', 17924], [4, 'HP', 17925], [4, 'Lenovo', 17925], [36, 'Huawei', 17925], [102, 'Apple', 17925], [4, 'ASUS', 17925], [10, 'Dell', 17925], [14, 'Sumsung', 17925], [20, 'xiaomi', 17925], [38, 'xiaomi', 17926], [92, 'Apple', 17926], [8, 'HP', 17926], [6, 'ASUS', 17926], [8, 'Dell', 17926], [2, 'Lenovo', 17926], [44, 'Huawei', 17926], [6, 'Sumsung', 17926]]

    feature_result.sort(key=lambda x: x[1], reverse=False)
    print(feature_result)

    attr = [str(datetime.datetime.fromtimestamp(ele[1]*86400))[:10] for ele in feature_result]
    value = [ele[0] for ele in feature_result]

    line = Line("折线图")
    line.add("", attr, value, mark_point=["average"])
    line.render()
