'''
Author: Jason Capehart
Created On: 7/26/13

Export Citibike Data
'''

from pymongo import MongoClient
import csv

connection = MongoClient('localhost')
db = connection["bike_raw"]

f = open("bike_dump.csv", "ab")
writer = csv.writer(f, delimiter = ",")

docks = db.bike_raw.find()

writer.writerow(["id", "availableBikes", "availableDocks", "totalDocks",
                 "city", "executionTime", "altitude", "stAddress1", 
                 "stAddress2", "longitude", "latitude", "lastCommunicationTime",
                 "postalCode", "statusValue", "testStation", "stationName",
                 "landMark", "statusKey", "location"]

for dock in docks:
    dock_list = [dock["id"]
                 , dock["availableBikes"]
                 , dock["availableDocks"]
                 , dock["totalDocks"]
                 , dock["city"]
                 , dock["executionTime"]
                 , dock["altitude"]
                 , dock["stAddress1"]
                 , dock["stAddress2"]
                 , dock["longitude"]
                 , dock["latitude"]
                 , dock["lastCommunicationTime"]
                 , dock["postalCode"]
                 , dock["statusValue"]
                 , dock["testStation"]
                 , dock["stationName"]
                 , dock["landMark"]
                 , dock["statusKey"]
                 , dock["location"]
                ]
    writer.writerow(dock_list)

f.close()
