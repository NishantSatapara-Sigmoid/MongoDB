import pymongo
from mongodb import *
import pprint

# Queries for movies collection
collection = database.movies

#Q1) Find top `N` movies - with the highest IMDB rating
result = collection.aggregate([ {"$addFields":{ "rating":{ "$convert": {"input": {"$getField":{ "field":{ "$literal": "$numberDouble"},"input": "$imdb.rating"}
                                                                                              }, "to": "double"} } } },
                               { "$sort": { "rating" : pymongo.DESCENDING}},
                                { "$limit" : 10} ,
                                { "$project": {  "_id":0,"title":1, "rating": 1}}] )




#Q2) Find top `N` movies - with the highest IMDB rating in a given year
result=collection.aggregate([
    { "$project":{"_id":0,"imdb.rating":{"$convert": {"input":{"$getField":{ "field":{ "$literal": "$numberDouble"},"input": "$imdb.rating"}
                                                                                              } , "to": "double"}},
    "yr":{"$convert":{"input":{"$getField":{ "field":{ "$literal": "$numberInt"},"input": "$year"}} , "to": "double"}} } },
    { "$match": { "yr":{ "$eq": 1976}}},
    {"$sort":{"imdb.rating":-1} },
    { "$limit": 10}])


#Q3) Find top `N` movies - with highest IMDB rating with number of votes > 1000
result=collection.aggregate([
    { "$project":{"_id":0,"imdb.rating":{"$convert": {"input":{"$getField":{ "field":{ "$literal": "$numberDouble"},"input": "$imdb.rating"}
                                                                                              } , "to": "double"}},
    "yr":{"$convert":{"input":{"$getField":{ "field":{ "$literal": "$numberInt"},"input": "$year"}} , "to": "double"}} ,
    "vote":{"$convert":{"input":{"$getField":{ "field":{ "$literal": "$numberInt"},"input": "$imdb.votes"}} , "to": "long"} }   } },
    { "$match": { "vote":{ "$gt": 1000}}},
    {"$sort":{"imdb.rating":-1} },
    { "$limit": 10}])




#04) with title matching a given pattern sorted by highest tomatoes ratings
result=collection.aggregate([
    { "$addFields":{ "result":{"$cond": {"if": {"$regexMatch": {"input": "$title", "regex":"Blacksmith Scene"}}, "then": 1, "else": 0 } }}},
    { "$match": { "result":{ "$eq": 1}}},
    { "$project":{"_id":0,"tomatoes_Rating":{"$convert":{"input": {"$getField":{ "field":{ "$literal": "$numberInt"},"input":"$tomatoes.viewer.rating"}} ,"to": "double", "onError": { "$convert":{ "input":{ "$getField":{ "field":{ "$literal": "$numberDouble"},"input":"$tomatoes.viewer.rating" }}, "to": "double"}}}} , "title":1} },
    {"$sort":{"tomatoes_Rating":-1} },
    { "$limit": 10}])



#05) Find top `N` directors - who created the maximum number of movies
result=collection.aggregate([ {"$unwind":"$directors"},
                              {"$group": { "_id": "$directors", "total_movie": { "$sum":1}}},
                              {"$sort":{ "total_movie": -1}},
                              {"$limit": 10}
                              ])



#06) Find top `N` directors - who created the maximum number of movies in given year
result=collection.aggregate([
                              { "$addFields":{"yr":{"$convert":{"input":{"$getField":{ "field":{ "$literal": "$numberInt"},"input": "$year"}} , "to": "double"}}}},
                              {"$match":{ "yr":{"$eq": 1977 }}},
                              {"$unwind":"$directors"},
                              {"$group": { "_id": "$directors", "total_movie": { "$sum":1}}},
                              {"$sort":{ "total_movie": -1}},
                              {"$limit": 10}
                              ])







#07)Find top `N` directors - who created the maximum number of movies for a given genres
genres="Animation"
result=collection.aggregate(
        [{"$unwind": "$directors"},
         {"$match": {"genres": {"$eq": genres}}},
         {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"director_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": 10}])



#08)Find top `N` actors - who starred in the maximum number of movies
result=collection.aggregate(
        [{"$unwind": "$cast"},
         {"$group": {"_id": {"actor_name": "$cast"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": 10}])




#09)Find top `N` actors - who starred in the maximum number of movies in given year
result=collection.aggregate([
         { "$addFields" : {"yr":{"$convert":{"input":{"$getField":{ "field":{ "$literal": "$numberInt"},"input": "$year"}} , "to": "double"}}}},
         {"$match":{ "yr":{"$eq": 1977 }}},
         {"$unwind": "$cast"},
         {"$group": {"_id": {"actor_name": "$cast"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": 10}])



#10) Find top `N` actors - who starred in the maximum number of movies for a given genres
genres="Animation"
result=collection.aggregate(
        [{"$unwind": "$cast"},
         {"$match": {"genres": {"$eq": genres}}},
         {"$group": {"_id": {"actor_name": "$cast"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": 10}])




#11) Find top `N` movies for each genre with the highest IMDB rating
result=collection.aggregate(
        [{"$addFields":{ "rating":{ "$convert": {"input": {"$getField":{ "field":{ "$literal": "$numberDouble"},"input": "$imdb.rating"}
                                                                                              }, "to": "double"} } } },
         {"$project": {"_id":0,"genres":1,"rating":1, "title":1}},
         {"$sort" : { "rating": -1}},
         {"$unwind": "$genres"},
         {"$group": {"_id":"$genres","title":{"$push":"$title"},"rating":{"$push":"$rating"}}},
         {"$project":{ "_id":1, "title":{"$slice":["$title",0,10]  },"rating":{"$slice":["$rating",0,10]  } }}
         ])

for i in result:
    pprint.pprint(i)
