import pandas as pd
import numpy as np
from datetime import datetime


# ── Response builder ──────────────────────────────────────────────────────────

def build_dataset_response(dataset: dict) -> dict:
    df = pd.DataFrame(dataset["data"])
    df = df.replace({np.nan: None})

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

    rows = len(df)
    cols = len(df.columns)
    null_count = int(df.isnull().sum().sum())
    dup_count = int(df.duplicated().sum())

    preview_limit = 5
    head_rows = df.head(preview_limit).to_dict(orient="records")

    created_at = dataset.get("createdAt", "")
    uploaded_at = created_at.isoformat() if isinstance(created_at, datetime) else str(created_at)

    return {
        "datasetId": str(dataset["_id"]),
        "source": {
            "type": dataset.get("type"),
            "fileName": dataset.get("fileName"),
            "fileMeta": {
                "size": dataset.get("fileSize", 0),
                "uploadedAt": uploaded_at
            }
        },
        "schema": {"columns": columns},
        "stats": {
            "rows": rows,
            "cols": cols,
            "nullCount": null_count,
            "errorCount": dup_count,
            "dataQualityScore": _quality_score(null_count, dup_count, rows)
        },
        "head": {
            "columns": list(df.columns),
            "rows": head_rows,
            "previewLimit": preview_limit
        }
    }


def _quality_score(null_count: int, dup_count: int, total_rows: int) -> int:
    if total_rows == 0:
        return 100
    penalty = ((null_count + dup_count) / (total_rows + 1)) * 100
    return max(0, min(100, int(100 - penalty)))


# ── Operations ────────────────────────────────────────────────────────────────

def apply_operation(df: pd.DataFrame, operation_type: str, params: dict) -> tuple:
    """
    Apply a named transformation to the dataframe.
    Returns (updated_df, result_message).
    Raises ValueError for unsupported or invalid operations.

    Supported operationTypes:
      REMOVE_NULLS, REMOVE_DUPLICATES, ADD_COLUMN, SPLIT_COLUMN,
      CHANGE_TYPE, TRANSFORM_TEXT, STATISTICS,
      RENAME_COLUMN, EXTRACT_DATE_PART, FLAG_COLUMN
    """
    op = operation_type.upper()

    # ── REMOVE_NULLS ──────────────────────────────────────────────
    if op == "REMOVE_NULLS":
        column = params.get("column", "All")
        before = len(df)
        if column == "All":
            df = df.dropna()
        else:
            _assert_col(df, column)
            df = df.dropna(subset=[column])
        return df, f"Removed {before - len(df)} rows with null values"

    # ── REMOVE_DUPLICATES ─────────────────────────────────────────
    elif op == "REMOVE_DUPLICATES":
        before = len(df)
        df = df.drop_duplicates()
        return df, f"Removed {before - len(df)} duplicate rows"

    # ── ADD_COLUMN ────────────────────────────────────────────────
    elif op == "ADD_COLUMN":
        col_name = params.get("column")
        formula = params.get("formula")
        default_value = params.get("defaultValue", 0)
        data_type = params.get("dataType")

        if not col_name:
            raise ValueError("'column' is required for ADD_COLUMN")

        if formula:
            try:
                # Build safe local namespace with column references
                local_vars = {
                    c.replace(" ", "_"): df[c]
                    for c in df.columns
                }
                safe_formula = formula
                for c in df.columns:
                    safe_formula = safe_formula.replace(c, c.replace(" ", "_"))
                df[col_name] = eval(safe_formula, {"__builtins__": {}}, local_vars)
            except Exception as e:
                raise ValueError(f"Could not evaluate formula '{formula}': {e}")
        else:
            df[col_name] = default_value

        if data_type and data_type != "object":
            try:
                df[col_name] = df[col_name].astype(data_type)
            except Exception:
                pass

        return df, f"Column '{col_name}' added"

    # ── SPLIT_COLUMN ──────────────────────────────────────────────
    elif op == "SPLIT_COLUMN":
        column = params.get("column")
        delimiter = params.get("delimiter", ",")
        new_names = params.get("newColumnNames", [])

        _assert_col(df, column)
        split_df = df[column].astype(str).str.split(delimiter, expand=True)

        for i in range(split_df.shape[1]):
            name = new_names[i] if i < len(new_names) else f"{column}_part{i + 1}"
            df[name] = split_df[i]

        return df, f"Column '{column}' split into {split_df.shape[1]} new columns"

    # ── CHANGE_TYPE ───────────────────────────────────────────────
    elif op == "CHANGE_TYPE":
        column = params.get("column")
        new_type = params.get("newType")
        fmt = params.get("format")
        error_handling = params.get("errorHandling", "coerce")

        _assert_col(df, column)
        if not new_type:
            raise ValueError("'newType' is required for CHANGE_TYPE")

        if "datetime" in new_type.lower():
            df[column] = pd.to_datetime(
                df[column],
                format=fmt if fmt and fmt != "infer" else None,
                errors=error_handling
            )
        else:
            try:
                df[column] = df[column].astype(new_type)
            except Exception as e:
                if error_handling == "raise":
                    raise ValueError(f"Type conversion failed: {e}")

        return df, f"Column '{column}' converted to {new_type}"

    # ── TRANSFORM_TEXT ────────────────────────────────────────────
    elif op == "TRANSFORM_TEXT":
        column = params.get("column")
        action = params.get("action", "trim")
        remove_special = params.get("removeSpecialChars", False)

        _assert_col(df, column)
        s = df[column].astype(str)

        if "trim" in action:
            s = s.str.strip()
        if "upper" in action:
            s = s.str.upper()
        elif "lower" in action:
            s = s.str.lower()
        elif "title" in action:
            s = s.str.title()

        if remove_special:
            s = s.str.replace(r"[^A-Za-z0-9\s]", "", regex=True)

        df[column] = s
        return df, f"Text in '{column}' transformed (action: {action})"

    # ── STATISTICS ────────────────────────────────────────────────
    elif op == "STATISTICS":
        column = params.get("column")
        metrics = params.get("metrics", ["mean", "median", "std", "min", "max"])
        group_by = params.get("groupBy")

        _assert_col(df, column)

        if not pd.api.types.is_numeric_dtype(df[column]):
            raise ValueError(f"Column '{column}' is not numeric")

        stat_fns = {
            "mean": lambda s: s.mean(),
            "median": lambda s: s.median(),
            "std": lambda s: s.std(),
            "min": lambda s: s.min(),
            "max": lambda s: s.max(),
            "sum": lambda s: s.sum(),
            "count": lambda s: s.count(),
        }

        if group_by:
            group_by = [group_by] if isinstance(group_by, str) else group_by
            for g in group_by:
                _assert_col(df, g)
            agg_result = df.groupby(group_by)[column].agg(
                [m for m in metrics if m in stat_fns]
            ).reset_index()
            for m in metrics:
                if m in agg_result.columns:
                    df[f"{column}_{m}"] = df[group_by].merge(
                        agg_result, on=group_by, how="left"
                    )[m].values
            return df, f"Statistics for '{column}' grouped by {group_by} added as columns"
        else:
            for m in metrics:
                if m in stat_fns:
                    df[f"{column}_{m}"] = stat_fns[m](df[column])
            return df, f"Statistics columns added for '{column}'"

    # ── RENAME_COLUMN ─────────────────────────────────────────────
    elif op == "RENAME_COLUMN":
        column = params.get("column")
        new_name = params.get("newName")

        _assert_col(df, column)
        if not new_name:
            raise ValueError("'newName' is required for RENAME_COLUMN")

        df = df.rename(columns={column: new_name})
        return df, f"Column '{column}' renamed to '{new_name}'"

    # ── EXTRACT_DATE_PART ─────────────────────────────────────────
    elif op == "EXTRACT_DATE_PART":
        column = params.get("column")
        part = params.get("part", "year")
        new_col = params.get("newColumn", f"{column}_{part}")

        _assert_col(df, column)
        dt = pd.to_datetime(df[column], errors="coerce")

        part_map = {
            "year": dt.dt.year,
            "month": dt.dt.month,
            "day": dt.dt.day,
            "weekday": dt.dt.day_name(),
        }

        if part not in part_map:
            raise ValueError(f"Unsupported date part '{part}'. Choose: {list(part_map.keys())}")

        df[new_col] = part_map[part]
        return df, f"Extracted {part} from '{column}' into '{new_col}'"

    # ── FLAG_COLUMN ───────────────────────────────────────────────
    elif op == "FLAG_COLUMN":
        column = params.get("column")
        threshold = params.get("threshold")
        operator = params.get("operator", ">")
        new_col = params.get("newColumn", f"{column}_flagged")

        _assert_col(df, column)
        if threshold is None:
            raise ValueError("'threshold' is required for FLAG_COLUMN")

        op_map = {
            ">":  df[column] > threshold,
            "<":  df[column] < threshold,
            ">=": df[column] >= threshold,
            "<=": df[column] <= threshold,
            "==": df[column] == threshold,
            "!=": df[column] != threshold,
        }

        if operator not in op_map:
            raise ValueError(f"Unsupported operator '{operator}'. Choose: {list(op_map.keys())}")

        df[new_col] = op_map[operator]
        return df, f"Flag column '{new_col}' created (where {column} {operator} {threshold})"

    else:
        raise ValueError(
            f"Unsupported operationType: '{operation_type}'. "
            "Supported: REMOVE_NULLS, REMOVE_DUPLICATES, ADD_COLUMN, SPLIT_COLUMN, "
            "CHANGE_TYPE, TRANSFORM_TEXT, STATISTICS, RENAME_COLUMN, "
            "EXTRACT_DATE_PART, FLAG_COLUMN"
        )


# ── Internal utils ────────────────────────────────────────────────────────────

def _assert_col(df: pd.DataFrame, col: str):
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")