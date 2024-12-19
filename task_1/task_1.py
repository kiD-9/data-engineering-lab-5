import csv
import pymongo
from pymongo import MongoClient
import json


def load_csv(path):
    extracted_csv = []
    with open(path, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            row['salary'] = int(row['salary'])
            row['id'] = int(row['id'])
            row['year'] = int(row['year'])
            row['age'] = int(row['age'])

            extracted_csv.append(row)

    return extracted_csv


def connect_to_db(db_name):
    return MongoClient()[db_name]


def connect_to_jobs_collection():
    db = connect_to_db("lab5")
    return db.jobs


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def first_query(jobs_collection):
    result = list(jobs_collection.find({}, {'_id': False}, limit=10).sort('salary', pymongo.DESCENDING))
    write_to_json('query_1.json', result)


def second_query(jobs_collection):
    result = list(jobs_collection.find({'age': {'$lt': 30}}, {'_id': False}, limit=15)
                  .sort('salary', pymongo.DESCENDING))
    write_to_json('query_2.json', result)


def third_query(jobs_collection):
    result = list(jobs_collection.find({
                          'city': 'Хихон',
                          'job': {'$in': ['Водитель', 'Продавец', 'Строитель']}
                      }, {'_id': False}, limit=10)
                  .sort('age', pymongo.ASCENDING))
    write_to_json('query_3.json', result)


def fourth_query(jobs_collection):
    result = jobs_collection.count_documents({
                          'age': {'$gt': 25, '$lt': 50},
                          'year': {'$gte': 2019, '$lte': 2022},
                          '$or': [
                              {'salary': {'$gt': 50000, '$lte': 75000}},
                              {'salary': {'$gt': 125000, '$lt': 150000}},
                          ]
                      })
    write_to_json('query_4.json', result)


# STEP: insert data
# data = load_csv("../data/task_1_item.csv")
# jobs_collection = connect_to_jobs_collection()
# jobs_collection.insert_many(data)

jobs_collection = connect_to_jobs_collection()
# STEP: queries
first_query(jobs_collection)
second_query(jobs_collection)
third_query(jobs_collection)
fourth_query(jobs_collection)