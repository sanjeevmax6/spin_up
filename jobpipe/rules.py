from __future__ import annotations

from pathlib import Path

import yaml

from jobpipe.models import JobRow, RuleSet


def load_rules(path: str | Path) -> RuleSet:
    file_path = Path(path)
    if not file_path.exists():
        return RuleSet()
    with file_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return RuleSet(**data)


def evaluate_row_rules(row: JobRow, rules: RuleSet) -> list[str]:
    errors: list[str] = []
    haystack_parts = [
        row.company,
        row.role_title,
        row.location,
        row.notes,
        row.job_description_text or "",
    ]
    haystack = " ".join(haystack_parts).lower()
    for disq in rules.hard_disqualifiers:
        if disq.lower() in haystack:
            errors.append(f"hard_disqualifier matched: {disq}")
    return errors

