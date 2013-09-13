'''
Created On: 6/15/2013
Author: Jason Capehart

Get Citibike Data
'''

import requests
import pymongo
from pymongo import MongoClient


def get_citibike():
    connection = MongoClient(host = 'localhost')
    db = connection["bike"]
    r = requests.get("https://citibikenyc.com/stations/json")
    bike_dict = r.json()
    executionTime = bike_dict["executionTime"]
    stations = bike_dict["stationBeanList"]
    for station in stations:
        station["executionTime"] = executionTime
        db.bike_raw.insert(station)


if __name__ == "__main__":
    get_citibike()
