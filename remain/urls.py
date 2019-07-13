from django.urls import path,re_path,include
from . import views
urlpatterns = [
    # path('',views.),
    path(r"remain/",views.remain,name='remain'),
]