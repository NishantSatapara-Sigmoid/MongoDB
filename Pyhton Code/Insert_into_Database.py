import pymongo
from mongodb import *
import pprint

def insert(collection, item_list):
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

doc = {
    "Name":"Nirav",
    "City":"Ahmedabad"
}

insert("comments",doc)
insert("movies",doc)
insert("theaters",doc)
insert("users",doc)
insert("sessions",doc)