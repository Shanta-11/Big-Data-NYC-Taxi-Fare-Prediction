from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
import json
from django.http import JsonResponse
import geopandas as gpd
from shapely.geometry import Point
import requests
from datetime import datetime

# Create your views here.
@api_view(['POST'])
def predict(request):
    
    data = request.data

    for k in data.keys():
        data[k] = float(data[k])

    shapefile = gpd.read_file("/mnt/6492D71D92D6F312/IITG/5th_Sem/DA331/Project/taxi_fare/PredAPI/taxi_zones.shp")
    shapefile = shapefile.to_crs(epsg=4326)
    target_point_PU = Point(data['PU_Longitude'], data['PU_Latitude']) 
    target_point_DO = Point(data['DO_Longitude'], data['DO_Latitude']) 
    
    
    

    containing_polygon_PU = shapefile[shapefile.geometry.contains(target_point_PU)]
    containing_polygon_DO = shapefile[shapefile.geometry.contains(target_point_DO)]

    data['PULocationID'] = int(shapefile.LocationID[containing_polygon_PU.index[0]])
    data['DOLocationID'] = int(shapefile.LocationID[containing_polygon_DO.index[0]])

    url = 'https://api.distancematrix.ai/maps/api/distancematrix/json'

    params = {'origins': f"{data['PU_Latitude']},{data['PU_Longitude']}",
              'destinations': f"{data['DO_Latitude']},{data['DO_Longitude']}",
              'key': 'CnZVUu6bMFiSyFXAR6RGNHvKG2Q2Wu25xKWeaWi5aZb8EupTcuWsEncuD3D344Pq'
              }
    
    r = requests.get(url = url, params = params)

    r = r.json()

    distance = int(r["rows"][0]["elements"][0]["distance"]["value"])/0.621371

    time = int(r["rows"][0]["elements"][0]["duration"]["value"]) * 60

    data['trip_distance'] = distance
    data['duration'] = time

    now = datetime.now()
    now.hour

    data['year'] = now.year
    data['month'] = now.month
    data['hour'] = now.hour
    data['day_of_week'] = now.weekday()

    
    
    data = [data]

    spark=SparkSession.builder.appName("Prediction").getOrCreate()
    df = spark.createDataFrame(data)
    feature_columns = ["trip_distance", "PULocationID", "DOLocationID", "year", "month", "hour", "day_of_week", "duration"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="indexedFeatures")
    df = assembler.transform(df)
    model = RandomForestRegressionModel.load("/mnt/6492D71D92D6F312/IITG/5th_Sem/DA331/Project/model")
    prediction = model.transform(df)
    prediction = prediction.collect()
    fare = prediction[0][-1]
    spark.stop()

    
    

    
    return JsonResponse(round(fare, 2), safe=False)

