import os
import urllib2
from celery import Celery
from celery.task import periodic_task
from datetime import timedelta
from lxml import objectify
from pymongo import MongoClient
from urlparse import urlparse

STATION_URL = "https://nextbike.net/maps/nextbike-official.xml?city=210"
REDIS_URL = os.environ.get("REDISTOGO_URL", "redis://localhost")
MONGO_URL = os.environ.get("MONGOLAB_URI")

if MONGO_URL:
    client = MongoClient(MONGO_URL)
    database_name = urlparse(MONGO_URL).path.lstrip("/")
    db = client[database_name]
else:
    client = MongoClient("localhost", 3001)
    db = client.meteor

app = Celery("tasks", broker=REDIS_URL)


@periodic_task(run_every=timedelta(minutes=5))
def station_status():
    resp = urllib2.urlopen(STATION_URL)
    data = objectify.fromstring(resp.read())

    stations_element = data.country.city.place
    stations = _get_stations(stations_element)

    for station in stations:
        db.stations.update({"uid": station["uid"]},
                           {"$set": station}, upsert=True)


def _get_stations(stations):
    return map(lambda s:
               {k.encode("utf-8"): v.encode("utf-8")
                for k, v in s.attrib.items()}, stations)
