from __future__ import annotations

from typing import Annotated, Any, TypedDict

from jobpipe.models import JobRow, RuleSet


def merge_dict(left: dict[str, Any] | None, right: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = dict(left or {})
    merged.update(dict(right or {}))
    return merged


def merge_error_dict(
    left: dict[str, list[str]] | None, right: dict[str, list[str]] | None
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {k: list(v) for k, v in (left or {}).items()}
    for key, values in (right or {}).items():
        merged.setdefault(key, [])
        merged[key].extend(values)
    return merged


class GraphArtifacts(TypedDict, total=False):
    resume_patch_md: str
    resume_full_md: str
    cover_letter_md: str
    linkedin_targets_md: str
    linkedin_notes_md: str
    cold_email_md: str
    resume_docx: str
    resume_pdf: str
    cover_letter_docx: str
    cover_letter_pdf: str
    manifest_json: str


class GraphState(TypedDict, total=False):
    row_number: int
    raw_row: dict[str, Any]
    row_context: JobRow
    resume_context: dict[str, Any]
    restrictions: dict[str, Any]
    rules: RuleSet
    artifacts: Annotated[GraphArtifacts, merge_dict]
    node_status: Annotated[dict[str, str], merge_dict]
    node_errors: Annotated[dict[str, list[str]], merge_error_dict]
    token_usage_by_node: Annotated[dict[str, dict[str, int]], merge_dict]
    output_dir: str
    app_config: Any
    llm_client: Any
    dry_run: bool
