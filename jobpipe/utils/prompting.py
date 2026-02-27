from __future__ import annotations

from pathlib import Path
from typing import Any


class _SafeDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def load_prompt(prompt_dir: str | Path, file_name: str) -> str:
    path = Path(prompt_dir) / file_name
    return path.read_text(encoding="utf-8")


def render_prompt(template: str, values: dict[str, Any]) -> str:
    safe_values = _SafeDict({k: stringify(v) for k, v in values.items()})
    return template.format_map(safe_values)


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return "\n".join([f"- {stringify(v)}" for v in value])
    if isinstance(value, dict):
        lines: list[str] = []
        for k, v in value.items():
            lines.append(f"{k}: {stringify(v)}")
        return "\n".join(lines)
    return str(value)

