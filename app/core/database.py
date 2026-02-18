from pymongo import MongoClient

MONGO_URI = "mongodb+srv://muthukumarann4545_db_user:PGhVMju0c6z7ohqE@muthu.aoum0db.mongodb.net/?appName=Muthu"

client = MongoClient(MONGO_URI)

db = client["adaptive_ai"]

users_collection = db["users"]
files_collection = db["files"]

print("Connected to MongoDB Atlas successfully!")
