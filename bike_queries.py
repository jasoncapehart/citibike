'''
Citibike queries
    - % of available bikes per dock per hour, day of week, and date
    - Avg % of available bikes by hour, per dock
    - Avg % of docks filled, by hour, per dock
    - How to identify "conveyors" (i.e. sources and sinks of bikes)?

TODO: Need to log the hour the aggregation is for per dock
'''

from pymongo import MongoClient
from dateutil.parser import *
from datetime import date

class BikeQuery():

    def __init__(self):
        connection = MongoClient('localhost')
        self.db = connection['bike']

    def normalize_bike_raw(self):
        docks = self.db.bike_raw.find({"$and":[{"statusValue":"In Service"}
                                               , {"totalDocks":{"$ne":0}}]})

        for dock in docks:
            time = parse(dock["executionTime"])
            hour = time.hour
            dow = time.weekday() # Mon = 0, Sun = 6
            month = time.month
            date = int(time.strftime("%Y%m%d"))
            pct_avail = dock["availableBikes"] / float(dock["totalDocks"])
            pct_docked = dock["availableDocks"] / float(dock["totalDocks"])
            self.db.bike.update({"_id":dock["_id"]}
                           , {"$set":{"id":dock["id"]
                                     , "availableDocks":dock["availableDocks"]
                                     , "totalDocks":dock["totalDocks"]
                                     , "longitude":dock["longitude"]
                                     , "latitude":dock["latitude"]
                                     , "statusValue":dock["statusValue"]
                                     , "availableBikes":dock["availableBikes"]
                                     , "stationName":dock["stationName"]
                                     , "pct_avail":pct_avail
                                     , "pct_docked": pct_docked
                                     , "hour":hour
                                     , "dow":dow
                                     , "month":month
                                     , "date":date}}
                           , upsert = True, multi = False)

    def summarize_hourly(self, days = 30):
        start_date = int(date.today().strftime("%Y%m%d")) - 30
        dock_ids = self.db.bike.distinct("id")
        for dock_id in dock_ids:
            tmp = self.db.bike.aggregate([{"$match":{"date":{"$gte":start_date}}}
                                      , {"$group":{"_id":"$hour"
                                          , "avg_avail":{"$avg":"$pct_avail"}
                                          , "avg_fill":{"$avg":"$pct_docked"}
                                          , "avg_bikes":{"$avg":"$availableBikes"}
                                          , "avg_docks":{"$avg":"$availableDocks"}
                                        }}
                                      ])
            results = tmp["result"]
            
            for result in results:
                hour = result["_id"]
                record_id = ("%s_%s") % (dock_id, hour)
                self.db.bike_hour.update({"_id":record_id}
                                    , {"$set":{ "id":dock_id
                                              , "avg_avail":result["avg_avail"]
                                              , "avg_fill":result["avg_fill"]
                                              , "avg_bikes":result["avg_bikes"]
                                              , "avg_docks":result["avg_docks"]
                                              , "hour":hour
                                              }
                                       }
                                    , upsert = True, multi = False)
