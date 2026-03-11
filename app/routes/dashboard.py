from fastapi import APIRouter, Header, HTTPException
from app.core.database import datasets_collection
from app.core.firebase import verify_firebase_token
from app.services.dashboard_service import generate_dashboard
import pandas as pd

router = APIRouter()


@router.post("/generate-dashboard")
async def generate_dashboard_route(
    body: dict,
    Authorization: str = Header(...)
):
    """
    Generate a dashboard config from the user's dataset + a natural language prompt.

    Body:
      prompt     : str  — e.g. "Marketing dashboard for CFO"
      isTemplate : bool — if True, returns a reusable template structure
    """
    token = Authorization.replace("Bearer ", "")
    decoded = verify_firebase_token(token)

    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded["uid"]

    prompt = body.get("prompt")
    is_template = body.get("isTemplate", False)

    if not prompt:
        raise HTTPException(status_code=400, detail="'prompt' is required")

    dataset = datasets_collection.find_one({"uid": uid})

    if not dataset:
        return {
            "success": False,
            "message": "No dataset found. Please upload a file first.",
            "data": None
        }

    df = pd.DataFrame(dataset["data"])

    try:
        dashboard_config = await generate_dashboard(df, prompt, is_template)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "success": True,
        "message": "Dashboard generated successfully",
        "data": dashboard_config
    }