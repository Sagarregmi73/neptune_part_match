from pymongo import MongoClient
import os

# You can adjust these from environment variables or defaults
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "mydatabase")

def get_mongo_client():
    """
    Returns a MongoClient instance connected to the MongoDB server.
    """
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    return client[MONGO_DB]  # Return the database instance if needed
