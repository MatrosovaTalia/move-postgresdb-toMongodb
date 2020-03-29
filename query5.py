import csv
import pymongo as pm
from datetime import datetime
import time

# to connect to db change db_name to your database name:
db_name = "dvdrental"
client = pm.MongoClient('localhost', 27017)
db = client[db_name]

start = time.time()
# For each actor get the list of actors,
# With whom they play
pipeline = [{"$group" :{"_id" : "$film_id", 
             "film_actors": {"$addToSet":"$actor_id"}}},
            {"$set" : {"actors" : '$film_actors'}},
            {"$unwind" : "$film_actors"},
            {"$unwind": "$actors"},
            {"$group" : {"_id": "$actors", 
            "filmed_with": {"$addToSet": "$film_actors"}}},
            {"$sort" : {"_id" : 1}}             
            ]
films = list(db.film_actors.aggregate(pipeline))

db.actors_filmed.drop()
db.actors_filmed.insert_many(films)

pipeline = []

# delete the actor itself from the list with whom they play
for i in range(1, 201):
    db.actors_filmed.update_one({"_id" : i},
                            {"$pull": {"filmed_with": i}})


# Find the Bacon's distance
pipeline = [{"$match" : {"_id": 1}}, {"$graphLookup": {
                "from": "actors_filmed",
                "startWith": "$_id",
                "connectFromField": "filmed_with",
                "connectToField": "_id",
                "depthField": "degree",
                "as": "connections"}}]

connections = list(db.actors_filmed.aggregate(pipeline))
actor_conn = [0] * 201
for i in range(0, 200):
    actor_conn[i + 1] = connections[0]["connections"][i]["degree"]
    
with open('query5.csv', mode='w') as outcsv:
    writer = csv.writer(outcsv, delimiter=',', quotechar='"',
                                  quoting=csv.QUOTE_MINIMAL)
    actor = db.actors.find_one({"_id" : 1})
    name = str(actor["first_name"]) + " " + str(actor["last_name"])
    # writer.writerow()
    writer.writerow(["Find distances for actor 1: ", name])
    writer.writerow([])

    for a_id in range(1, 201):
        actor = db.actors.find_one({"_id": a_id})
        first_name = actor['first_name']
        last_name = actor['last_name']
        name = str(first_name) + " " + str(last_name)
        writer.writerow([name, actor_conn[a_id - 1]])

db.actors_filmed.drop()
end = time.time()
# print(end - start)