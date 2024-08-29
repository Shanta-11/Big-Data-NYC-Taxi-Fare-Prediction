from django.db import models

# Create your models here.
class TripModel(models.Model):
    trip_distance = models.IntegerField()
    PULocationID = models.IntegerField()
    DOLocationID = models.IntegerField()
    year = models.IntegerField()
    month = models.IntegerField()
    hour = models.IntegerField()
    day_of_week = models.IntegerField()
    duration = models.IntegerField()

    def __str__(self):
        return self.PULocationID