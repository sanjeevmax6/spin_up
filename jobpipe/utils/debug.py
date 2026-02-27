from __future__ import annotations

import json
from typing import Any

from jobpipe.state import GraphState


def pretty_print_state(state: GraphState, label: str | None = None) -> None:
    """Pretty-print top-level GraphState keys with shallow values only."""
    title = label or "GraphState"
    print(f"\n{'=' * 24} {title} {'=' * 24}")
    print(json.dumps(_normalize_graph_state(state), indent=2, ensure_ascii=False))
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


def _normalize_graph_state(state: GraphState) -> dict[str, Any]:
    return {str(k): _normalize_value(v) for k, v in state.items()}


def _normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        # Shallow dict: do not recurse deeply into nested objects.
        return {str(k): _shallow_repr(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_shallow_repr(v) for v in value]
    if isinstance(value, tuple):
        return [_shallow_repr(v) for v in value]
    return _object_ref(value)


def _shallow_repr(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return f"<dict len={len(value)}>"
    if isinstance(value, list):
        return f"<list len={len(value)}>"
    if isinstance(value, tuple):
        return f"<tuple len={len(value)}>"
    return _object_ref(value)


def _object_ref(value: Any) -> str:
    return f"<{value.__class__.__name__} at 0x{id(value):x}>"
