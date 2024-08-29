from django.contrib import admin
from django.urls import path, include
from . import views

urlpatterns = [
    path('api/', views.predict),
    # path('', include('PredAPI.urls'))
]