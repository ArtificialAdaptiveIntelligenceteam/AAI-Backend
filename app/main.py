from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from bson import ObjectId

from app.core.database import files_collection
from app.routes.auth import router as auth_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)


@app.post("/upload-excel")
async def upload_excel(
    file: UploadFile = File(...),
    uid: str = Form(...)
):
    try:
        filename = file.filename.lower()

        if filename.endswith(".xlsx"):
            df = pd.read_excel(file.file, engine="openpyxl")

        elif filename.endswith(".csv"):
            df = pd.read_csv(file.file)

        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        data = df.fillna("").to_dict(orient="records")

        result = files_collection.insert_one({
            "uid": uid,
            "filename": file.filename,
            "data": data
        })

        return {"file_id": str(result.inserted_id)}

    except Exception as e:
        print("UPLOAD ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get-excel/{file_id}")
def get_excel(file_id: str):
    doc = files_collection.find_one({"_id": ObjectId(file_id)})

    if not doc:
        raise HTTPException(status_code=404, detail="File not found")

    return doc["data"]
