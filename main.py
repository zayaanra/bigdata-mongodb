import pymongo
import json
import csv
import pandas as pd

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["bigdata"]

# ------------------------- Q0: Load all datasets ------------------------- #
# TODO: Show proof?

# with open("restaurants.json", "r") as f:
#     restaurantData = json.load(f)
# db.restaurantData.insert_many(restaurantData)

# with open("meteorites.json", "r") as f:
#     meteoritesData = json.load(f)
# db.meteoritesData.insert_many(meteoritesData)

# with open("durham-nc-foreclosure-2006-2016.json") as f:
#     foreclosureData = json.load(f)
# db.foreclosureData.insert_many(foreclosureData)

# durhamCountyData = pd.read_csv('Restaurants_in_Durham_County_NC.csv', delimiter=';')   # loading csv file
# durhamCountyData.to_json('Restaurants_in_Durham_County_NC.json')
# requesting = []
# with open("Restaurants_in_Durham_County_NC.json", "r") as f:
#     for jsonObj in f:
#         myDict = json.loads(jsonObj)
#         requesting.append(myDict)
# db.durhamCountyData.insert_many(requesting)

# worldCitiesData = pd.read_csv('worldcities.csv', delimiter=';')   # loading csv file
# worldCitiesData.to_json('worldcities.json')
# requesting = []
# with open("worldcities.json", "r") as f:
#     for jsonObj in f:
#         myDict = json.loads(jsonObj)
#         requesting.append(myDict)
# db.worldCitiesData.insert_many(requesting)


# ------------------------- Q1: Write MongoDB queries for restaurants.json ------------------------- #
# 1. Count the number of documents in the collection.
# print("Number of documents in restaurants.json collection: ", db.restaurantData.count_documents({}))

# 2. Display all the documents in the collection.
# for doc in db.restaurantData.find():
#     print(doc)

# # 3. Display: restaurant_id, name, borough and cuisine for all the documents
# for doc in db.restaurantData.find({}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)

# # 4. Display: restaurant_id, name, borough and cuisine, but exclude field _id, for all the documents in the collection
# for doc in db.restaurantData.find({}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1, "_id": 0}):
#     print(doc)

# 5. Display: restaurant_id, name, borough and zip code, exclude the field _id for all the documents in the collection.
# for doc in db.restaurantData.find({}, {"restaurant_id": 1, "name": 1, "borough": 1, "address.zipcode": 1, "_id": 0}):
#     print(doc)

# 6. Display all the restaurants in the Bronx.
# for doc in db.restaurantData.find({"borough": "Bronx"}):
#     print(doc)

# 7. Display the first 5 restaurants in the Bronx
# for doc in db.restaurantData.find({"borough": "Bronx"}).limit(5):
#     print(doc)

# 8. Display the second 5 restaurants (skipping the first 5) in the Bronx.
# for doc in db.restaurantData.find({"borough": "Bronx"}).skip(5).limit(5):
#     print(doc)

# 9. Find the restaurants with any score more than 85.
# for doc in db.restaurantData.find({"grades.score": {"$gt": 85}}):
#     print(doc)

# 10.Find the restaurants that achieved score, more than 80 but less than 100.
# TODO: Incorrect results
# for doc in db.restaurantData.find({"grades.score": {"$gt": 80, "$lt": 100}}):
#     print(doc)

# 11.Find the restaurants which locate in longitude value less than -95.754168.
# for doc in db.restaurantData.find({"address.coord.0": {"$lt": -95.754168}}):
#     print(doc)

# 12.Find the restaurants that do not prepare any cuisine of 'American' and their grade score more than 70 and longitude less than -65.754168.
# for doc in db.restaurantData.find({"cuisine": {"$ne": "American "}, "grades.score": {"$gt": 70}, "address.coord.0": {"$lt": -65.754168}}):
#     print(doc)

# 13.Find the restaurants which do not prepare any cuisine of 'American' and achieved a score more than 70 and located in the longitude less than -65.754168. (without using $and operator).
# for doc in db.restaurantData.find({"cuisine": {"$ne": "American "}, "grades.score": {"$gt": 70}, "address.coord.0": {"$lt": -65.754168}}):
#     print(doc)

# 14.Find the restaurants which do not prepare any cuisine of 'American ' and achieved a grade point 'A' and not in the borough of Brooklyn, sorted by cuisine in descending order.
# for doc in db.restaurantData.find({"cuisine": {"$ne": "American "}, "grades.grade": {"$eq": 'A'}, "borough": {"$ne": "Brooklyn"}}).sort("cuisine", -1):
#     print(doc)

# 15.Find the restaurant Id, name, borough and cuisine for those restaurants which contain 'Wil' as first three letters for its name.
# for doc in db.restaurantData.find({"name": {"$regex": "^Wil"}}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)

# 16.Find the restaurant Id, name, borough and cuisine for those restaurants which contain 'ces' as last three letters for its name.
# for doc in db.restaurantData.find({"name": {"$regex": "ces$"}}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)

# 17.Find the restaurant Id, name, borough and cuisine for those restaurants which contain 'Reg' as three letters somewhere in its name.
# for doc in db.restaurantData.find({"name": {"$regex": "Reg"}}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)

# 18.Find the restaurants which belong to the borough Bronx and prepared either American or Chinese dish
# for doc in db.restaurantData.find({"borough": "Bronx", "cuisine": {"$in": ["American ", "Chinese"]}}):
#     print(doc)

# 19.Find the restaurant Id, name, borough and cuisine for those restaurants which belong to the boroughs of Staten Island or Queens or Bronx or Brooklyn.
# for doc in db.restaurantData.find({"borough": {"$in": ["Staten Island", "Queens", "Bronx", "Brooklyn"]}}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)

# 20.Find the restaurant Id, name, borough and cuisine for those restaurants which are not belonging to the borough Staten Island or Queens or Bronx or Brooklyn
# for doc in db.restaurantData.find({"borough": {"$nin": ["Staten Island", "Queens", "Bronx", "Brooklyn"]}}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)


# 21.Find the restaurant Id, name, borough and cuisine for those restaurants which achieved a score below 10.
# for doc in db.restaurantData.find({"grades.score": {"$lt": 10}}, {"restaurant_id": 1, "name": 1, "borough": 1, "cuisine": 1}):
#     print(doc)

# 22.Find the restaurant Id, name, borough and cuisine for those restaurants which prepared dish except 'American' and 'Chinese' or restaurant's name begins with letter 'Wil'.

# 23.Find the restaurant Id, name, and grades for those restaurants which achieved a grade of "A" and scored 11 on an ISODate "2014-08-11T00:00:00Z" among many of survey dates.

# 24.Find the restaurant Id, name and grades for those restaurants where the 2nd element of grades array contains a grade of "A" and score 9 on an ISODate "2014-08-11T00:00:00Z".

# 25.Find the restaurant Id, name, address and geographical location for those restaurants where 2nd element of coordinates contains a value which is more than 42 and up to 52.

client.close()