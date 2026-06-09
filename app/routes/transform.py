from fastapi import APIRouter, Header, HTTPException
from app.core.database import datasets_collection
from app.core.firebase import verify_firebase_token
from app.services.dataset_service import build_dataset_response, apply_operation
import pandas as pd
import numpy as np

router = APIRouter(prefix="/transform")

MAX_TEXT_SUGGESTIONS = 5


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_user_dataset(token: str) -> dict:
    decoded = verify_firebase_token(token)
    if not decoded:
        raise HTTPException(status_code=401, detail="Invalid Firebase token")

    uid = decoded["uid"]
    dataset = datasets_collection.find_one({"uid": uid})

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return dataset


def _save_df(dataset: dict, df: pd.DataFrame) -> dict:
    """Persist updated dataframe to DB and return the refreshed document."""
    df = df.replace({np.nan: None})
    datasets_collection.update_one(
        {"_id": dataset["_id"]},
        {"$set": {"data": df.to_dict(orient="records")}}
    )
    # Return updated dataset without an extra round-trip query
    dataset["data"] = df.to_dict(orient="records")
    return dataset


def _is_date(val) -> bool:
    """
    Returns True only if the value looks like a real date string,
    not a plain integer or float that pandas would coerce into a date.
    """
    s = str(val).strip()
    # Reject bare numbers — pd.to_datetime("42") succeeds but it's not a date
    if s.lstrip("-").isdigit():
        return False
    try:
        pd.to_datetime(s, infer_datetime_format=True)
        return True
    except Exception:
        return False


def _compute_quality_score(df: pd.DataFrame) -> int:
    """
    Score out of 100 based on:
      - Null ratio     → up to -60 penalty
      - Duplicate ratio → up to -40 penalty
    """
    rows, cols = df.shape
    total_cells = max(rows * cols, 1)

    null_count = int(df.isnull().sum().sum())
    dup_count = int(df.duplicated().sum())

    null_ratio = null_count / total_cells
    dup_ratio = dup_count / max(rows, 1)

    score = 100 - (null_ratio * 60) - (dup_ratio * 40)
    return max(0, round(score))


def _generate_suggestions_with_ops(df: pd.DataFrame) -> list:
    """
    Generates suggestions with internal _operation/_params fields.
    Strip these keys before returning to the client.
    """
    suggestions = []
    id_counter = 1

    # ── Null values ──────────────────────────────────────────────────────────
    null_count = int(df.isnull().sum().sum())
    if null_count > 0:
        suggestions.append({
            "id": id_counter,
            "title": "Remove missing values",
            "description": f"{null_count} null value{'s' if null_count > 1 else ''} detected across the dataset",
            "confidence": "high" if null_count > 50 else "medium",
            "rows_affected": null_count,
            "_operation": "REMOVE_NULLS",
            "_params": {"column": "All"}
        })
        id_counter += 1

    # ── Duplicate rows ───────────────────────────────────────────────────────
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        suggestions.append({
            "id": id_counter,
            "title": "Remove duplicate rows",
            "description": f"{dup_count} duplicate row{'s' if dup_count > 1 else ''} detected",
            "confidence": "high",
            "rows_affected": dup_count,
            "_operation": "REMOVE_DUPLICATES",
            "_params": {}
        })
        id_counter += 1

    # ── String columns that look like dates ──────────────────────────────────
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().head(20)
        if len(sample) == 0:
            continue
        date_hits = sum(1 for val in sample if _is_date(val))
        if date_hits >= len(sample) * 0.8:
            suggestions.append({
                "id": id_counter,
                "title": f'Convert "{col}" to Date type',
                "description": f'Column "{col}" contains date-like values stored as strings',
                "confidence": "medium",
                "column": col,
                "_operation": "CHANGE_TYPE",
                "_params": {
                    "column": col,
                    "newType": "datetime64",
                    "format": "infer",
                    "errorHandling": "coerce"
                }
            })
            id_counter += 1
            break  # surface one at a time to avoid overwhelming the user

    # ── String columns with mixed casing / leading-trailing whitespace ────────
    text_suggestion_count = 0
    for col in df.select_dtypes(include="object").columns:
        if text_suggestion_count >= MAX_TEXT_SUGGESTIONS:
            break
        sample = df[col].dropna().head(50).astype(str)
        has_ws = sample.str.startswith(" ").any() or sample.str.endswith(" ").any()
        has_mixed = (
            (sample.str.upper() != sample).any()
            and (sample.str.lower() != sample).any()
        )
        if has_ws or has_mixed:
            issues = []
            if has_ws:
                issues.append("extra whitespace")
            if has_mixed:
                issues.append("mixed casing")
            suggestions.append({
                "id": id_counter,
                "title": f'Standardize text in "{col}"',
                "description": f'Detected {" and ".join(issues)} in column "{col}"',
                "confidence": "medium",
                "column": col,
                "_operation": "TRANSFORM_TEXT",
                "_params": {
                    "column": col,
                    "action": "trim_and_upper",
                    "removeSpecialChars": False
                }
            })
            id_counter += 1
            text_suggestion_count += 1

    # ── Numeric column statistics ─────────────────────────────────────────────
    # Pick the column with the most variance (most interesting to summarise)
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        col = max(num_cols, key=lambda c: df[c].std(skipna=True) or 0)
        suggestions.append({
            "id": id_counter,
            "title": f'Show statistics for "{col}"',
            "description": f'Generate mean, median, std, min, max for "{col}"',
            "confidence": "low",
            "column": col,
            "_operation": "STATISTICS",
            "_params": {
                "column": col,
                "metrics": ["mean", "median", "std", "min", "max"]
            }
        })
        id_counter += 1  # kept consistent with rest of function

    return suggestions


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/get-dataset")
async def transform_get_dataset(
    Authorization: str = Header(...)
):
    token = Authorization.replace("Bearer ", "")
    dataset = _get_user_dataset(token)

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
    dataset = _get_user_dataset(token)

    df = pd.DataFrame(dataset["data"])
    raw = _generate_suggestions_with_ops(df)

    # Strip internal keys before sending to client
    clean = [{k: v for k, v in s.items() if not k.startswith("_")} for s in raw]

    return {
        "success": True,
        "message": "AI suggestions generated successfully",
        "data": {
            "suggestions": clean,
            "count": len(clean)
        }
    }


@router.post("/apply-suggestion")
async def apply_suggestion(
    body: dict,
    Authorization: str = Header(...)
):
    """
    Apply a suggestion by its id.
    Suggestions are re-derived from the live dataset so the operation
    always reflects the current state, not a stale client-side snapshot.
    """
    token = Authorization.replace("Bearer ", "")
    dataset = _get_user_dataset(token)

    suggestion_id = body.get("id")
    if suggestion_id is None:
        raise HTTPException(status_code=400, detail="'id' is required")

    df = pd.DataFrame(dataset["data"])
    suggestions_raw = _generate_suggestions_with_ops(df)
    matched = next((s for s in suggestions_raw if s.get("id") == suggestion_id), None)

    if not matched:
        raise HTTPException(
            status_code=404,
            detail=f"No active suggestion found with id={suggestion_id}. "
                   "The dataset may have changed — please refresh suggestions."
        )

    try:
        df, message = apply_operation(df, matched["_operation"], matched.get("_params", {}))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    updated = _save_df(dataset, df)

    return {
        "success": True,
        "message": message,
        "data": build_dataset_response(updated)
    }


@router.post("/apply-operation")
async def apply_operation_route(
    body: dict,
    Authorization: str = Header(...)
):
    """
    Apply a manual operation to the dataset.

    Supported operationTypes:
      REMOVE_NULLS        params: { column: "All" | "<col>" }
      REMOVE_DUPLICATES   params: {}
      ADD_COLUMN          params: { column, formula?, dataType?, defaultValue? }
      SPLIT_COLUMN        params: { column, delimiter, newColumnNames? }
      CHANGE_TYPE         params: { column, newType, format?, errorHandling? }
      TRANSFORM_TEXT      params: { column, action, removeSpecialChars? }
      STATISTICS          params: { column, metrics[], groupBy? }
      RENAME_COLUMN       params: { column, newName }
      EXTRACT_DATE_PART   params: { column, part, newColumn? }
      FLAG_COLUMN         params: { column, threshold, operator?, newColumn? }
    """
    token = Authorization.replace("Bearer ", "")
    dataset = _get_user_dataset(token)

    operation_type = body.get("operationType")
    params = body.get("parameters", {})

    if not operation_type:
        raise HTTPException(status_code=400, detail="'operationType' is required")

    df = pd.DataFrame(dataset["data"])

    try:
        df, message = apply_operation(df, operation_type, params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    updated = _save_df(dataset, df)

    return {
        "success": True,
        "message": f"Operation {operation_type} applied — {message}",
        "data": build_dataset_response(updated)
    }