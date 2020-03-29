import csv
import pymongo
import time

# to connect to db change db_name to your database name:
db_name = "dvdrental"
client = pymongo.MongoClient('localhost', 27017)
db = client[db_name]

start = time.time()
pipeline = [{"$group": {"_id": "$film_id", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}]

with open('query3.csv', 'w', newline='') as outcsv:
    writer = csv.writer(outcsv)
    writer.writerow(["Film_id", "Title", "Category", 
                     "Number of times the film was rented"])

    films_rented_num = list(db.rentals.aggregate(pipeline))
    for film in films_rented_num:
        film_id = film["_id"]

        film_info = db.films.find_one({"_id": film_id})
        title = film_info["title"]

        rental_info = db.rentals.find_one({"film_id": film_id})
        category = rental_info["category"]

        rented_num = film["count"]
        writer.writerow([film_id, title, category, rented_num])

end = time.time()
# print(end - start)

