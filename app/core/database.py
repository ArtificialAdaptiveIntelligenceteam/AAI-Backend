from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)

# DB
db = client["adaptive_ai"]

# collections
users_collection = db["users"]
files_collection = db["files"]

print("Connected to MongoDB Atlas successfully!")
