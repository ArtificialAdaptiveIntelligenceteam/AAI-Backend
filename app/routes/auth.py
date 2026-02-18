from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from app.core.database import users_collection
from typing import Optional

router = APIRouter()


# ==============================
# 📦 Models
# ==============================

class UserCreate(BaseModel):
    name: str
    email: EmailStr
    uid: str


class UserLogin(BaseModel):
    email: EmailStr


# ==============================
# 🚀 Signup Route
# ==============================

@router.post("/signup")
async def signup(user: UserCreate):
    try:
        # 🔍 Check if user already exists (by email OR uid)
        existing_user = users_collection.find_one({
            "$or": [
                {"email": user.email},
                {"uid": user.uid}
            ]
        })

        if existing_user:
            raise HTTPException(
                status_code=400,
                detail="User already exists"
            )

        # 📝 Insert new user
        result = users_collection.insert_one({
            "name": user.name,
            "email": user.email,
            "uid": user.uid
        })

        return {
            "success": True,
            "message": "User created successfully",
            "user": {
                "id": str(result.inserted_id),
                "name": user.name,
                "email": user.email,
                "uid": user.uid
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print("SIGNUP ERROR:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


# ==============================
# 🔐 Signin Route
# ==============================

@router.post("/signin")
async def signin(user: UserLogin):
    try:
        # 🔍 Check if user exists in MongoDB
        existing_user = users_collection.find_one({"email": user.email})

        if not existing_user:
            raise HTTPException(
                status_code=404,
                detail="User not found in database"
            )

        return {
            "success": True,
            "message": "User login successful",
            "user": {
                "name": existing_user["name"],
                "email": existing_user["email"],
                "uid": existing_user["uid"]
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print("SIGNIN ERROR:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
