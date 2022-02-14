import pymongo
from mongodb import *
import pprint


# Queries for theaters collection
collection = database.theaters

#01)Top 10 cities with the maximum number of theatres

result=collection.aggregate([{"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
                                 {"$project": {"location.address.city": 1, "count": 1}},
                                 {"$sort": {"count": -1}},
                                 {"$limit": 10}])
for i in result:
    pprint.pprint(i)


#02)top 10 theatres nearby given coordinates

database.theaters.createIndex( { "location.geo" : "2dsphere" } )
result=collection.aggregate([{"$geoNear": { "near": {"type": "Point", "coordinates": [-85.24, 48.85]},
                                                  "maxDistance": 10000000, "distanceField": "distance"}},
                                    {"$project": {"location.addess.city": 1, "_id": 0, "location.geo.coordinates": 1,"distance":1}},
                                    {"$limit": 10}])
for i in result:
    pprint.pprint(i)




