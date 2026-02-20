from fastapi import FastAPI, UploadFile, File, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import ObjectId
import pandas as pd
import numpy as np

from app.core.database import datasets_collection
from app.core.firebase import verify_firebase_token
from app.routes.auth import router as auth_router
from app.routes.transform import router as transform_router

app = FastAPI()

app.include_router(auth_router)
app.include_router(transform_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================
# Upload Dataset
# =========================================
@app.post("/upload-file")
async def upload_file(
    file: UploadFile = File(...),
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    decoded = verify_firebase_token(token)

    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded["uid"]

    try:
        filename = file.filename.lower()

        if filename.endswith(".csv"):
            df = pd.read_csv(file.file)
            file_type = "csv"

        elif filename.endswith(".xlsx"):
            df = pd.read_excel(file.file, engine="openpyxl")
            file_type = "excel"

        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        df = df.replace({np.nan: None})

        # Remove previous dataset (one-to-one logic)
        datasets_collection.delete_many({"uid": uid})

        # Build schema
        columns = []
        for col in df.columns:
            dtype = str(df[col].dtype)

            if "int" in dtype or "float" in dtype:
                col_type = "number"
            elif "datetime" in dtype:
                col_type = "date"
            else:
                col_type = "string"

            columns.append({"name": col, "type": col_type})

        # Stats
        rows = len(df)
        cols = len(df.columns)
        null_count = int(df.isnull().sum().sum())

        preview_limit = 5
        head_rows = df.head(preview_limit).to_dict(orient="records")

        dataset_doc = {
            "uid": uid,
            "fileName": file.filename,
            "type": file_type,
            "data": df.to_dict(orient="records"),
            "createdAt": datetime.utcnow()
        }

        result = datasets_collection.insert_one(dataset_doc)

        return {
            "success": True,
            "message": "File uploaded successfully",
            "data": {
                "datasetId": str(result.inserted_id),
                "source": {
                    "type": file_type,
                    "fileName": file.filename,
                    "fileMeta": {
                        "size": file.size,
                        "uploadedAt": datetime.utcnow().isoformat()
                    }
                },
                "schema": {
                    "columns": columns
                },
                "stats": {
                    "rows": rows,
                    "cols": cols,
                    "nullCount": null_count,
                    "errorCount": 0,
                    "dataQualityScore": 80
                },
                "head": {
                    "columns": list(df.columns),
                    "rows": head_rows,
                    "previewLimit": preview_limit
                }
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================================
# Get Dataset
# =========================================
@app.post("/get-dataset")
async def get_dataset(
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    decoded = verify_firebase_token(token)

    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded["uid"]

    dataset = datasets_collection.find_one({"uid": uid})

    if not dataset:
        return {
            "success": False,
            "message": "Could get dataset. Please try again.",
            "data": None
        }

    return {
        "success": True,
        "message": "",
        "data": {
            "datasetId": str(dataset["_id"])
        }
    }


# =========================================
# Remove Dataset
# =========================================
@app.delete("/remove-dataset")
async def remove_dataset(
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    decoded = verify_firebase_token(token)

    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded["uid"]

    result = datasets_collection.delete_one({"uid": uid})

    if result.deleted_count == 0:
        return {
            "success": False,
            "message": "File could not be deleted.",
            "data": None
        }

    return {
        "success": True,
        "message": "File deleted successfully.",
        "data": None
    }