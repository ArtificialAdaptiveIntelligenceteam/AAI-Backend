from fastapi import APIRouter, Header, HTTPException
from app.core.database import users_collection
from app.core.firebase import verify_firebase_token
from datetime import datetime

router = APIRouter()


@router.post("/getUserDetails")
async def get_user_details(
    user: dict,
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    decoded = verify_firebase_token(token)

    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded.get("uid")
    email = decoded.get("email")

    existing_user = users_collection.find_one({"uid": uid})

    if existing_user:
        return {
            "success": True,
            "message": "",
            "data": {
                "uid": existing_user["uid"],
                "email": existing_user["email"],
                "displayName": existing_user.get("displayName"),
                "photoURL": existing_user.get("photoURL")
            }
        }

    # Create new user (Google auth handled by Firebase)
    new_user = {
        "uid": uid,
        "email": email,
        "displayName": user.get("displayName"),
        "photoURL": user.get("photoUrl"),
        "provider": "google",
        "createdAt": datetime.utcnow()
    }

    users_collection.insert_one(new_user)

    return {
        "success": True,
        "message": "",
        "data": {
            "uid": uid,
            "email": email,
            "displayName": new_user.get("displayName"),
            "photoURL": new_user.get("photoURL")
        }
    }