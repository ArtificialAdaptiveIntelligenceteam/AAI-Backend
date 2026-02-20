from fastapi import APIRouter, Header, HTTPException
from app.core.database import datasets_collection
from app.core.firebase import verify_firebase_token
from app.services.dataset_service import build_dataset_response, generate_mock_suggestions
import pandas as pd

router = APIRouter(prefix="/transform")


def get_user_dataset(token: str):
    decoded = verify_firebase_token(token)
    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded["uid"]
    dataset = datasets_collection.find_one({"uid": uid})

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return dataset


@router.post("/get-dataset")
async def transform_get_dataset(
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    dataset = get_user_dataset(token)

    return {
        "success": True,
        "message": "",
        "data": build_dataset_response(dataset)
    }


@router.post("/get-suggestions")
async def get_suggestions(
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    dataset = get_user_dataset(token)

    df = pd.DataFrame(dataset["data"])

    suggestions = generate_mock_suggestions(df)

    return {
        "success": True,
        "message": "AI suggestions generated successfully",
        "data": {
            "suggestions": suggestions,
            "count": len(suggestions)
        }
    }


@router.post("/apply-suggestion")
async def apply_suggestion(
    body: dict,
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    dataset = get_user_dataset(token)

    df = pd.DataFrame(dataset["data"])
    suggestion_id = body.get("id")

    if suggestion_id == 1:
        df = df.dropna()
    elif suggestion_id == 2:
        df = df.drop_duplicates()

    datasets_collection.update_one(
        {"_id": dataset["_id"]},
        {"$set": {"data": df.to_dict(orient="records")}}
    )

    updated = datasets_collection.find_one({"_id": dataset["_id"]})

    return {
        "success": True,
        "message": "Suggestion applied",
        "data": build_dataset_response(updated)
    }


@router.post("/apply-operation")
async def apply_operation(
    body: dict,
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    dataset = get_user_dataset(token)

    df = pd.DataFrame(dataset["data"])

    operation = body.get("operationType")

    if operation == "REMOVE_NULLS":
        df = df.dropna()

    elif operation == "REMOVE_DUPLICATES":
        df = df.drop_duplicates()

    else:
        raise HTTPException(status_code=400, detail="Unsupported operation")

    datasets_collection.update_one(
        {"_id": dataset["_id"]},
        {"$set": {"data": df.to_dict(orient="records")}}
    )

    updated = datasets_collection.find_one({"_id": dataset["_id"]})

    return {
        "success": True,
        "message": f"Operation {operation} applied",
        "data": build_dataset_response(updated)
    }