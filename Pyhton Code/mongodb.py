from pymongo import MongoClient   # importing mongo driver
import json  # to load json file
from bson import ObjectId

# connection to local host
client = MongoClient('localhost', 27017)

# database creation
database = client.sample_mflix

# array for json file
json_data = ["comments", "movies", "sessions", "theaters", "users"]

# Load each data set in respective collection
dbnames = client.list_database_names()
if 'sample_mflix' not in dbnames:
    for collection in json_data:
        item_list = []
        with open(f'{collection}.json') as f:
            for json_obj in f:
                if json_obj:
                    my_dict = json.loads(json_obj)
                    my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
                    item_list.append(my_dict)
        if collection == "comments":
            database.comments.insert_many(item_list)
        elif collection == "movies":
            database.movies.insert_many(item_list)
        elif collection == "sessions":
            database.sessions.insert_many(item_list)
        elif collection == "theaters":
            database.theaters.insert_many(item_list)
        elif collection == "users":
            database.users.insert_many(item_list)



