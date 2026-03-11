import os
import json
import httpx
import pandas as pd
import numpy as np

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


def _build_schema_summary(df: pd.DataFrame) -> str:
    """Summarise the dataframe schema + sample stats for the AI prompt."""
    lines = [f"Dataset: {len(df)} rows × {len(df.columns)} columns\n"]
    lines.append("Columns:")

    for col in df.columns:
        dtype = str(df[col].dtype)
        if "int" in dtype or "float" in dtype:
            col_type = "number"
            stats = f"min={df[col].min()}, max={df[col].max()}, mean={round(df[col].mean(), 2)}"
        elif "datetime" in dtype:
            col_type = "date"
            stats = f"range={df[col].min()} to {df[col].max()}"
        else:
            col_type = "string"
            unique = df[col].nunique()
            top = df[col].value_counts().head(3).index.tolist()
            stats = f"unique={unique}, top={top}"

        null_pct = round(df[col].isnull().mean() * 100, 1)
        lines.append(f"  - {col} ({col_type}): {stats}, nulls={null_pct}%")

    return "\n".join(lines)


async def generate_dashboard(
    df: pd.DataFrame,
    prompt: str,
    is_template: bool
) -> dict:
    """
    Call Claude to generate a dashboard config JSON from the prompt + schema.
    Returns a structured dashboard config dict.
    """
    if not ANTHROPIC_API_KEY:
        raise EnvironmentError("ANTHROPIC_API_KEY not set in environment")

    schema_summary = _build_schema_summary(df)

    system_prompt = """You are a data visualisation expert. 
Given a dataset schema and a user prompt, return a JSON dashboard configuration.

The JSON must follow this exact structure and contain ONLY valid JSON — no markdown, no explanation:

{
  "title": "string",
  "description": "string",
  "isTemplate": boolean,
  "layout": "grid" | "single-column",
  "charts": [
    {
      "id": "string",
      "type": "bar" | "line" | "pie" | "scatter" | "table" | "metric",
      "title": "string",
      "description": "string",
      "xAxis": "column_name or null",
      "yAxis": "column_name or null",
      "groupBy": "column_name or null",
      "aggregation": "sum" | "mean" | "count" | "min" | "max" | null,
      "columns": ["col1", "col2"] // for table type only
    }
  ],
  "filters": [
    {
      "id": "string",
      "column": "column_name",
      "type": "select" | "date-range" | "number-range"
    }
  ]
}

Choose chart types and columns that make sense for the prompt and data. 
Include 3-6 charts. Add relevant filters based on available columns.
"""

    user_message = f"""Dataset schema:
{schema_summary}

User prompt: "{prompt}"
isTemplate: {str(is_template).lower()}

Generate a dashboard configuration JSON for this request."""

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 2000,
                "system": system_prompt,
                "messages": [
                    {"role": "user", "content": user_message}
                ]
            }
        )

    if response.status_code != 200:
        raise Exception(f"Anthropic API error {response.status_code}: {response.text}")

    result = response.json()
    raw_text = result["content"][0]["text"].strip()

    # Strip markdown code fences if present
    if raw_text.startswith("```"):
        raw_text = raw_text.split("```")[1]
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
        raw_text = raw_text.strip()

    try:
        dashboard_config = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to parse dashboard JSON from AI response: {e}\nRaw: {raw_text[:300]}")

    # Attach metadata
    dashboard_config["isTemplate"] = is_template
    dashboard_config["generatedFrom"] = {
        "prompt": prompt,
        "datasetRows": len(df),
        "datasetCols": len(df.columns)
    }

    return dashboard_config