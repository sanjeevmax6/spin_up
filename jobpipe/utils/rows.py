from __future__ import annotations

from typing import Any


def parse_rows_csv(rows_csv: str) -> list[int]:
    rows: list[int] = []
    for part in rows_csv.split(","):
        stripped = part.strip()
        if not stripped:
            continue
        rows.append(int(stripped))
    return sorted(set(rows))


def validate_required_columns(raw_row: dict[str, Any]) -> list[str]:
    normalized = {k.strip().lower() for k in raw_row.keys() if k}
    missing = [col for col in ["company", "role_title", "location"] if col not in normalized]
    has_description_source = any(
        col in normalized
        for col in ["job_posting_url", "job_description_url", "job_description_text"]
    )
    if not has_description_source:
        missing.append("job_posting_url_or_job_description_url")
    return missing

