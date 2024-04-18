import os
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv('TOKEN')

MONGO_DB = "mongodb://localhost:27017/"

DATABASE = "salaries"

COLLECTION = 'sample_collection.bson'

FORMAT_INTERVAL = {
    "hour": "%Y-%m-%dT%H:00:00",
    "day": "%Y-%m-%dT00:00:00",
    "month": "%Y-%m-01T00:00:00",
}
