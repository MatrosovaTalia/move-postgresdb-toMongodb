import csv
import pymongo as pm
import time

# to connect to db change db_name to your database name:
db_name = "dvdrental"
client = pm.MongoClient('localhost', 27017)
db = client[db_name]

start = time.time()
n_actors = db.actors.count_documents({})
table = [[0] * n_actors for i in range(n_actors)]
# db.film_actors.aggregate([])
pipeline = [{"$group" :{"_id" : "$film_id", 
             "actors": {"$addToSet":"$actor_id"} }},
            {"$sort": {"_id": 1, "actors" : 1}}]
films = list(db.film_actors.aggregate(pipeline))

for i in range (1, n_actors + 1):
    for film in films:
        if i in film["actors"]:
            for j in film["actors"]:
                table[i-1][j-1] += 1

actors = db.actors.find({})
names = []
names.append(" ")
with open('query2.csv', mode='w') as outcsv:
    writer = csv.writer(outcsv, delimiter=',', quotechar='"',
                               quoting=csv.QUOTE_MINIMAL)
    for actor in actors:
        names.append(actor['first_name'] + " " + actor['last_name'])
    writer.writerow(names)

    for i in range(n_actors):
        csv_row = [names[i]]
        for j in range(n_actors):
            if i == j:
                csv_row.append("x")
            else:
                csv_row.append(table[i][j])
        writer.writerow(csv_row)

end = time.time()
# print(end - start)


