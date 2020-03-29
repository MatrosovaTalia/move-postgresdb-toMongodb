import pymongo
from datetime import datetime
import collections 
import csv
import time

# to connect to db change db_name to your database name:
db_name = "dvdrental"
client = pymongo.MongoClient('localhost', 27017)
db = client[db_name]

start = time.time()
latest_date = list(db.rentals.find().sort("rental_last_update", -1))[0]
current_year = latest_date['rental_last_update'].year
start_date = datetime(current_year, 1, 1)
end_date = datetime(current_year+1, 1, 1)


categories = {}
rentals = db.rentals.find({'rental_date': 
                          {'$gte': start_date, '$lt': end_date}})    
for rental in rentals:
    category_id = rental['category_id']
    customer_id = rental['customer_id']
    categories.setdefault(customer_id, set()).add(category_id)

with open('query1.csv', 'w', newline='') as outcsv:
    writer = csv.writer(outcsv)
    writer.writerow(["Customer id", "First name", "Last name"])
    for c_id in sorted(categories.keys()):
        if len(categories[c_id]) >= 2:
            customer = db.customers.find_one({"_id": c_id})        
            writer.writerow([c_id, customer["first_name"], customer["last_name"]])

end = time.time()
# print(end - start)

