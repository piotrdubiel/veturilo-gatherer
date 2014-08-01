import os
import urllib2
from celery import Celery
from celery.task import periodic_task
from datetime import timedelta
from lxml import objectify
from pymongo import MongoClient
from urlparse import urlparse
from datetime import datetime
from firebase import firebase

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
firebase = firebase.FirebaseApplication("https://luminous-fire-8583.firebaseio.com", None)


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

    firebase.put("/stations", "stations", stations)


def _get_stations(stations):
    mapped_stations = map(lambda s:
                          {k.encode("utf-8"): v.encode("utf-8")
                           for k, v in s.attrib.items()}, stations)
    return map(lambda s: {"bike_numbers": s["bike_numbers"].split(",") if "bike_numbers" in s else [],
                          "bike_racks": s["bike_racks"] if "bike_racks" in s else 0,
                          "bikes": s["bikes"],
                          "location": [float(s["lng"]), float(s["lat"])],
                          "name": s["name"],
                          "spot": s["spot"],
                          "number": s["number"],
                          "uid": s["uid"]}, mapped_stations)


station_status()
