from django.db import models


from django.db import models

class SpikeData(models.Model):
    dateTime = models.CharField(max_length=100)  # To store dateTime as string
    value = models.FloatField(null=True, blank=True)  # To store the numeric value

    def __str__(self):
        return f"{self.dateTime}: {self.value}"

from django.db import models

class StationName(models.Model):
    station_name = models.CharField(max_length=255)

    def __str__(self):
        return self.station_name
