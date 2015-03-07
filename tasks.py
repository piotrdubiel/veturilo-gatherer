import os
import urllib2
from celery import Celery
from celery.task import periodic_task
from datetime import timedelta
from lxml import objectify
from pymongo import MongoClient
from urlparse import urlparse
from datetime import datetime

STATION_URL = "https://nextbike.net/maps/nextbike-official.xml?city=210"
REDIS_URL = os.environ.get("REDISTOGO_URL", "redis://localhost")
MONGO_URL = os.environ.get("MONGOLAB_URI")

if MONGO_URL:
    client = MongoClient(MONGO_URL)
    database_name = urlparse(MONGO_URL).path.lstrip("/")
    db = client[database_name]
else:
    client = MongoClient("localhost", 27017)
    db = client.wearturilo

app = Celery("tasks", broker=REDIS_URL)


@periodic_task(run_every=timedelta(minutes=5))
def station_status():
    resp = urllib2.urlopen(STATION_URL)
    xml = resp.read()
    data = objectify.fromstring(xml)

    stations_element = data.country.city.place
    stations = _get_stations(stations_element)

    print len(stations)
    db.stations.insert({"stations": stations, "updated": datetime.now()})

    for station in stations:
        db.latest.update({"uid": station["uid"]},
                         {"$set": station}, upsert=True)


def _get_stations(stations):
    mapped_stations = map(lambda s:
                          {k.encode("utf-8"): v.encode("utf-8")
                           for k, v in s.attrib.items()}, stations)
    return map(lambda s: {"bikeIds": s["bike_numbers"].split(",") if "bike_numbers" in s else [],
                          "rackNumber": s["bike_racks"] if "bike_racks" in s else 0,
                          "bikeNumber": _get_bike_count(s["bikes"]),
                          "lat": float(s["lat"]),
                          "lng": float(s["lng"]),
                          "stationName": s["name"],
                          "stationNumber": s["number"],
                          "uid": s["uid"]}, mapped_stations)


def _get_bike_count(count):
    if count == '1':
        return "ONE"
    elif count == '2':
        return "TWO"
    elif count == '3':
        return "THREE"
    elif count == '4':
        return "FOUR"
    elif count == '5+':
        return "MORE"
    else:
        return "NONE"

station_status()
