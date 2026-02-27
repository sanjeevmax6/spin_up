from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from jobpipe.state import GraphState


def pretty_print_state(state: GraphState, label: str | None = None) -> None:
    """Pretty-print GraphState in a readable JSON form."""
    title = label or "GraphState"
    print(f"\n{'=' * 24} {title} {'=' * 24}")
    print(json.dumps(_normalize(state), indent=2, ensure_ascii=False))
    print("=" * (50 + len(title)))


def preview_merged_state(state: GraphState, updates: dict[str, Any]) -> GraphState:
    """Merge node updates onto current state for debug viewing."""
    merged: dict[str, Any] = dict(state)
    for key, value in updates.items():
        if key in {"node_status", "token_usage_by_node", "artifacts"} and isinstance(value, dict):
            base = dict(merged.get(key, {}))
            base.update(value)
            merged[key] = base
            continue
        if key == "node_errors" and isinstance(value, dict):
            base = {k: list(v) for k, v in dict(merged.get(key, {})).items()}
            for err_key, err_values in value.items():
                base.setdefault(err_key, [])
                base[err_key].extend(err_values)
            merged[key] = base
            continue
        merged[key] = value
    return merged  # type: ignore[return-value]


def _normalize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, tuple):
        return [_normalize(v) for v in value]
    if hasattr(value, "model_dump"):
        try:
            return _normalize(value.model_dump())
        except Exception:  # noqa: BLE001
            return str(value)
    if hasattr(value, "__dict__"):
        try:
            return _normalize(vars(value))
        except Exception:  # noqa: BLE001
            return str(value)
    return str(value)

