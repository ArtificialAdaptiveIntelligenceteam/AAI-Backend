import pandas as pd
import numpy as np
from datetime import datetime


def build_dataset_response(dataset):
    df = pd.DataFrame(dataset["data"])
    df = df.replace({np.nan: None})

    # Schema
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

    return {
        "datasetId": str(dataset["_id"]),
        "source": {
            "type": dataset["type"],
            "fileName": dataset["fileName"],
            "fileMeta": {
                "size": 0,
                "uploadedAt": dataset["createdAt"].isoformat()
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


def generate_mock_suggestions(df: pd.DataFrame):
    suggestions = []
    id_counter = 1

    # Null values
    null_count = int(df.isnull().sum().sum())
    if null_count > 0:
        suggestions.append({
            "id": id_counter,
            "title": "Remove missing values",
            "description": f"{null_count} null values detected",
            "confidence": "medium",
            "rows_affected": null_count
        })
        id_counter += 1

    # Duplicate rows
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        suggestions.append({
            "id": id_counter,
            "title": "Remove duplicates",
            "description": f"{dup_count} duplicate rows detected",
            "confidence": "low",
            "rows_affected": dup_count
        })
        id_counter += 1

    return suggestions