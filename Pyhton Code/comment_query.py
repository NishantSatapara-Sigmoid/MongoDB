import pymongo
from mongodb import *
import pprint

# Queries for comment collection
collection = database.comments

# 1) Find top 10 users who made the maximum number of comments
result = (collection.aggregate([{"$group": {"_id": "$name" , "count": {"$sum": 1}}}, {"$sort": {"count": pymongo.DESCENDING}}, {"$limit": 10}]))
for i in result:
    pprint.pprint(i)

print("\n")

# 2) Find top 10 movies with most comments
result = (collection.aggregate(
    [{"$group": {"_id": "$movie_id", "count": {"$sum": 1}}}, {"$sort": {"count": pymongo.DESCENDING}}, {"$limit": 10}]))
for i in result:
    pprint.pprint(database.movies.find_one({"_id": ObjectId(i["_id"]["$oid"])}, {"_id": 0, "title": 1}))

# 3) Given a year find the total number of comments created each month in that year
result=collection.aggregate([ { "$project":
                                { "_id":0,
                                   "date":{"$toDate":{"$convert":{"input":{"$getField": { "field":{ "$literal": "$numberLong"},
                                    "input":{"$getField":{ "field":{ "$literal": "$date"},"input": "$date"}}}} , "to": "long"}}}
                                 }
                              },
                              { "$group": {"_id":{"year":{"$year":"$date"},"month":{"$month":"$date"}},"total comment":{"$sum":1}}},
                              { "$match": { "_id.year":{ "$eq": 1976}}},
                              { "$sort": { "_id.month" : 1}}
                            ])
for i in result:
    pprint.pprint(i)


