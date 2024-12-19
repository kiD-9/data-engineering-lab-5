import msgpack
import pymongo
from pymongo import MongoClient
import json


def load_msgpack(path):
    with open(path, 'rb') as file:
        return msgpack.load(file)


def connect_to_db(db_name):
    return MongoClient()[db_name]


def connect_to_jobs_collection():
    db = connect_to_db("lab5")
    return db.jobs


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


# 	вывод минимальной, средней, максимальной salary
def first_query(jobs_collection):
    aggregation = [
        {
            "$group": {
                "_id": "result",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        },
        {
            "$project": {
                "_id": 0
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_1.json', result)


# 	вывод количества данных по представленным профессиям
def second_query(jobs_collection):
    aggregation = [
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1},
            }
        },
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_2.json', result)


# 	вывод минимальной, средней, максимальной salary по городу
def third_query(jobs_collection):
    aggregation = [
        {
            "$group": {
                "_id": "$city",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_3.json', result)


# 	вывод минимальной, средней, максимальной salary по профессии
def fourth_query(jobs_collection):
    aggregation = [
        {
            "$group": {
                "_id": "$job",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_4.json', result)


# 	вывод минимального, среднего, максимального возраста по городу
def fifth_query(jobs_collection):
    aggregation = [
        {
            "$group": {
                "_id": "$city",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_5.json', result)


# 	вывод минимального, среднего, максимального возраста по профессии
def sixth_query(jobs_collection):
    aggregation = [
        {
            "$group": {
                "_id": "$job",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_6.json', result)


# 	вывод максимальной заработной платы при минимальном возрасте
def seventh_query(jobs_collection):
    job = jobs_collection.find_one(sort=[('age', pymongo.ASCENDING), ('salary', pymongo.DESCENDING)])
    result = {'age': job['age'], 'salary': job['salary']}
    write_to_json('query_7.json', result)


# 	вывод минимальной заработной платы при максимальной возрасте
def eighth_query(jobs_collection):
    job = jobs_collection.find_one(sort=[('age', pymongo.DESCENDING), ('salary', pymongo.ASCENDING)])
    result = {'age': job['age'], 'salary': job['salary']}
    write_to_json('query_8.json', result)


# 	вывод минимального, среднего, максимального возраста по городу, при условии...
def ninth_query(jobs_collection):
    aggregation = [
        {
            "$match": {
                "salary": {"$gt": 50000}
            }
        },
        {
            "$group": {
                "_id": "$city",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "avg_age": pymongo.DESCENDING
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_9.json', result)


# 	вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах...
def tenth_query(jobs_collection):
    aggregation = [
        {
            "$match": {
                "city": {"$in": ["Рига", "Вильнюс", "Хихон"]},
                "job": {"$in": ["Инженер", "Водитель", "Продавец"]},
                '$or': [
                    {'age': {'$gt': 18, '$lt': 25}},
                    {'age': {'$gt': 50, '$lt': 65}},
                ]
            }
        },
        {
            "$group": {
                "_id": "result",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        },
        {
            "$project": {
                "_id": 0
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_10.json', result)


# 	произвольный запрос с $match, $group, $sort
def eleventh_query(jobs_collection):
    aggregation = [
        {
            "$match": {
                "age": {"$gt": 30, "$lt": 50}
            }
        },
        {
            "$group": {
                "_id": "$year",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        },
        {
            "$sort": {
                "avg_salary": pymongo.DESCENDING
            }
        }
    ]
    result = list(jobs_collection.aggregate(aggregation))
    write_to_json('query_11.json', result)


# STEP: insert data
# data = load_msgpack("../data/task_2_item.msgpack")
# jobs_collection = connect_to_jobs_collection()
# jobs_collection.insert_many(data)

jobs_collection = connect_to_jobs_collection()
# STEP: queries
first_query(jobs_collection)
second_query(jobs_collection)
third_query(jobs_collection)
fourth_query(jobs_collection)
fifth_query(jobs_collection)
sixth_query(jobs_collection)
seventh_query(jobs_collection)
eighth_query(jobs_collection)
ninth_query(jobs_collection)
tenth_query(jobs_collection)
eleventh_query(jobs_collection)