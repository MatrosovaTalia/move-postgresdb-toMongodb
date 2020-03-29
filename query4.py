import csv
import pymongo as pm
from datetime import datetime
import time

# to connect to db change db_name to your database name:
db_name = "dvdrental"
client = pm.MongoClient('localhost', 27017)
db = client[db_name]

start = time.time()
# Choose the custumer to make recommendation
c_id = 4000
while(not (0 < c_id < 201)):
    print("Enter customer id: natural number from 1 to 200")
    c_id = int(input())

# Find the "current" year
cur_date = list(db.rentals.find().sort("last_update", -1))[0]
cur_year = cur_date['rental_last_update'].year

# Find the time interval: 3 years, 
# strarting from two years before the current year
start_date = datetime(cur_year - 2, 1, 1)
end_date = datetime(cur_year + 1, 1, 1)

# Find unique categories that customer with _id = c_id has watched during 
# ^ that interval
customer_films = []
categories = db.rentals.distinct("category", {'customer_id': c_id,
                          'rental_date': {'$gte': start_date,
                                          '$lt': end_date}})
# Randomly choose several categories from all the categories that 
# The customer likes
chosen_categories = []
for i in range(len(categories)):
    if (i % 5 == 1):
        chosen_categories.append(categories[i])

# Find film_id's that the customers, who like the same categories
# Watched during the last year (excluding the given customer)
start_date = datetime(cur_year, 1, 1)
end_date = datetime(cur_year + 1, 1, 1)

recommendations = list(db.rentals.distinct("film_id", {'rental_date': {'$gte': start_date, '$lt': end_date},
                        "category": {"$in" : chosen_categories},
                        "customer_id" : {"$ne": c_id}}))
# Find film titels from film_id from the previous step,
# Choose those, for which retal rate is greater than 2 
# And sort them by rental rate in descending order
rec_films = db.films.find({"_id": {"$in": recommendations}, 
                               "rental_rate": 
                               {"$gte": 2}}).sort("rental_rate", -1).limit(4)

# Choose 5 films from the obtained list (with highest rental rate)
file = open("query4.txt", "w")
file.write("Top 5 recommended films for customer " + str(c_id) + ":\n")   
i = 1    
for f in rec_films:  
    recommendation = (" Rilm_id: " + str(f["_id"]) + "; Title: " 
                      + str(f["title"]) + "; Rental rate: "
                      + str(f["rental_rate"]) + ";\n")
    file.write(str(i) + recommendation)
    i += 1

file.write("\nThe recommendation is based on rentals of the customers with"
           " the same categories \nchoice during the last year, sorted by"
           " rental rate.\n\n*Customer categories preferences are several"
           " (the number varies) categories \nfrom all the categories that given"
           " customer has watched during the last 3 years.")
file.close()
end = time.time()
# print(end - start)

