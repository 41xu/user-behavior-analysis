from django.urls import path,re_path,include
from . import views
urlpatterns = [
    # path('',views.),
    path(r"event/",views.event,name='event'),
]