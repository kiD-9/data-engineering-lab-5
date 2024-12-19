import pickle
import pymongo
from pymongo import MongoClient
import json


def load_pkl(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def connect_to_db(db_name):
    return MongoClient()[db_name]


def connect_to_jobs_collection():
    db = connect_to_db("lab5")
    return db.jobs


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


# 	удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
def first_query(jobs_collection):
    jobs_collection.delete_many({
        '$or': [
            {'salary': {'$lt': 25000}},
            {'salary': {'$gt': 175000}},
        ]
    })


# 	увеличить возраст (age) всех документов на 1
def second_query(jobs_collection):
    jobs_collection.update_many({}, {
        '$inc': {
            'age': 1
        }
    })


# 	поднять заработную плату на 5% для произвольно выбранных профессий
def third_query(jobs_collection):
    jobs_collection.update_many({
        "job": {"$in": ["Инженер", "Водитель", "Продавец"]}
    }, {
        '$mul': {
            'salary': 1.05
        }
    })


# 	поднять заработную плату на 7% для произвольно выбранных городов
def fourth_query(jobs_collection):
    jobs_collection.update_many({
        "city": {"$in": ["Рига", "Вильнюс", "Авилес"]}
    }, {
        '$mul': {
            'salary': 1.07
        }
    })


# 	поднять заработную плату на 10% для выборки по сложному предикату
def fifth_query(jobs_collection):
    jobs_collection.update_many({
        "city": "Авилес",
        "job": {"$in": ["Повар", "Медсестра", "Продавец"]},
        "age": {'$gt': 18, '$lt': 45}
    }, {
        '$mul': {
            'salary': 1.1
        }
    })


# 	удалить из коллекции записи по произвольному предикату
def sixth_query(jobs_collection):
    jobs_collection.delete_many({
        'age': {'$gte': 20, '$lte': 25},
        'year': {'$lt': 2010},
        "job": {"$in": ["IT-специалист", "Бухгалтер", "Медсестра"]},
    })


# STEP: insert data
# data = load_pkl("../data/task_3_item.pkl")
# jobs_collection = connect_to_jobs_collection()
# jobs_collection.insert_many(data)

jobs_collection = connect_to_jobs_collection()
# STEP: queries
# first_query(jobs_collection)
# second_query(jobs_collection)
# third_query(jobs_collection)
# fourth_query(jobs_collection)
# fifth_query(jobs_collection)
# sixth_query(jobs_collection)