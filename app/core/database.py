from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise Exception("❌ MONGO_URI not found in .env")

client = MongoClient(MONGO_URI)

db = client["adaptive_ai"]

users_collection = db["users"]
datasets_collection = db["datasets"]

print("✅ Connected to MongoDB Atlas")