import bson
import pymongo

from config import MONGO_DB, COLLECTION


def add_data():
    with pymongo.MongoClient(MONGO_DB) as client:
        db = client['salaries']
        with open(COLLECTION, 'rb') as file:
            data = file.read()
            decode_data = bson.decode_all(data)

            collection = db['salary']
            collection.insert_many(decode_data)


if __name__ == "__main__":
    add_data()
