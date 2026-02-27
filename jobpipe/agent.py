from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from pydantic import ValidationError

from jobpipe.models import JobRow
from jobpipe.rules import evaluate_row_rules
from jobpipe.sheets import fetch_job_description
from jobpipe.state import GraphState
from jobpipe.utils.prompting import load_prompt, render_prompt
from jobpipe.utils.rendering import write_docx, write_markdown, write_pdf
from jobpipe.utils.rows import validate_required_columns
from jobpipe.utils.debug import pretty_print_state


def ingest_row_node(state: GraphState) -> dict[str, Any]:
    node = "ingest_row_node"
    raw_row = dict(state.get("raw_row", {}))
    row_number = int(state.get("row_number", 0))
    missing = validate_required_columns(raw_row)
    if missing:
        return _mark_failed(state, node, f"missing required columns: {', '.join(missing)}")
    try:
        row = JobRow.from_sheet_row(row_number=row_number, raw=raw_row)
    except ValidationError as exc:
        msgs = [err["msg"] for err in exc.errors()]
        return _mark_failed(state, node, f"validation error: {'; '.join(msgs)}")
    updates = _mark_succeeded(state, node)
    updates["row_context"] = row
    return updates


def validate_context_node(state: GraphState) -> dict[str, Any]:
    node = "validate_context_node"
    row = state.get("row_context")
    app_config = state.get("app_config")
    if row is None or app_config is None:
        return _mark_failed(state, node, "missing row context or app config")

    if not row.job_description_text and row.job_description_url:
        try:
            row.job_description_text = fetch_job_description(row.job_description_url)
        except Exception as exc:  # noqa: BLE001
            return _mark_failed(state, node, f"unable to fetch job description URL: {exc}")

    rules = state.get("rules")
    if rules is None:
        return _mark_failed(state, node, "rules not loaded")

    rule_errors = evaluate_row_rules(row, rules)
    if rule_errors:
        return _mark_failed(state, node, "; ".join(rule_errors))

    resume_context = _load_yaml_file(app_config.resume_context_path)
    if not resume_context:
        return _mark_failed(
            state,
            node,
            f"resume context not found or empty: {app_config.resume_context_path}",
        )
    restrictions = _load_yaml_file(app_config.restrictions_path)

    updates = _mark_succeeded(state, node)
    updates["resume_context"] = resume_context
    updates["restrictions"] = restrictions
    updates["row_context"] = row
    return updates


def resume_node(state: GraphState) -> dict[str, Any]:
    node = "resume_node"
    app_config = state.get("app_config")
    row = state.get("row_context")
    llm_client = state.get("llm_client")
    resume_context = state.get("resume_context", {})
    restrictions = state.get("restrictions", {})
    if app_config is None or row is None or llm_client is None:
        return _mark_failed(state, node, "missing graph context for resume generation")

    try:
        shared_restrictions = load_prompt(app_config.prompt_dir, "shared_restrictions.prompt.md")
        selection_template = load_prompt(app_config.prompt_dir, "resume_selection.prompt.md")
        rewrite_template = load_prompt(app_config.prompt_dir, "resume_rewrite.prompt.md")
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"unable to load resume templates: {exc}")

    selection_prompt = render_prompt(
        selection_template,
        {
            "shared_restrictions": shared_restrictions,
            "row_context": json.dumps(row.model_dump(), indent=2),
            "resume_context": json.dumps(resume_context, indent=2),
            "extra_restrictions": json.dumps(restrictions, indent=2),
        },
    )
    try:
        selection_text, usage_1 = llm_client.complete(selection_prompt)
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"resume selection failed: {exc}")

    selected_experience_ids, selected_project_ids = _extract_selected_ids(
        selection_text, resume_context
    )
    selected_context, unselected_context = _split_context(
        resume_context, selected_experience_ids, selected_project_ids
    )
    rewrite_prompt = render_prompt(
        rewrite_template,
        {
            "shared_restrictions": shared_restrictions,
            "row_context": json.dumps(row.model_dump(), indent=2),
            "selected_context": json.dumps(selected_context, indent=2),
            "extra_restrictions": json.dumps(restrictions, indent=2),
        },
    )
    try:
        rewritten_text, usage_2 = llm_client.complete(rewrite_prompt)
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"resume rewrite failed: {exc}")

    resume_patch_md = _build_resume_patch_md(
        selected_experience_ids, selected_project_ids, rewritten_text
    )
    resume_full_md = _build_resume_full_md(
        resume_context, unselected_context, rewritten_text
    )
    merged_usage = {
        "input_tokens": int(usage_1.get("input_tokens", 0)) + int(usage_2.get("input_tokens", 0)),
        "output_tokens": int(usage_1.get("output_tokens", 0)) + int(usage_2.get("output_tokens", 0)),
        "total_tokens": int(usage_1.get("total_tokens", 0)) + int(usage_2.get("total_tokens", 0)),
    }
    return _mark_succeeded(
        state,
        node,
        artifacts={"resume_patch_md": resume_patch_md, "resume_full_md": resume_full_md},
        usage=merged_usage,
    )


def cover_letter_node(state: GraphState) -> dict[str, Any]:
    node = "cover_letter_node"
    app_config = state.get("app_config")
    row = state.get("row_context")
    llm_client = state.get("llm_client")
    artifacts = dict(state.get("artifacts", {}))
    restrictions = state.get("restrictions", {})
    if app_config is None or row is None or llm_client is None:
        return _mark_failed(state, node, "missing graph context for cover letter")
    if not artifacts.get("resume_full_md"):
        return _mark_failed(state, node, "resume output missing; cannot build cover letter")

    try:
        shared_restrictions = load_prompt(app_config.prompt_dir, "shared_restrictions.prompt.md")
        template = load_prompt(app_config.prompt_dir, "cover_letter.prompt.md")
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"unable to load cover letter template: {exc}")

    prompt = render_prompt(
        template,
        {
            "shared_restrictions": shared_restrictions,
            "row_context": json.dumps(row.model_dump(), indent=2),
            "resume_full_md": artifacts.get("resume_full_md", ""),
            "extra_restrictions": json.dumps(restrictions, indent=2),
        },
    )
    try:
        text, usage = _run_with_retry(
            lambda: llm_client.complete(prompt),
            retries=int(app_config.non_resume_node_retries),
            backoff_seconds=float(app_config.non_resume_retry_backoff_seconds),
        )
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"cover letter generation failed: {exc}")
    return _mark_succeeded(state, node, artifacts={"cover_letter_md": text}, usage=usage)


def linkedin_search_node(state: GraphState) -> dict[str, Any]:
    node = "linkedin_search_node"
    app_config = state.get("app_config")
    row = state.get("row_context")
    llm_client = state.get("llm_client")
    restrictions = state.get("restrictions", {})
    if app_config is None or row is None or llm_client is None:
        return _mark_failed(state, node, "missing graph context for linkedin search")

    try:
        shared_restrictions = load_prompt(app_config.prompt_dir, "shared_restrictions.prompt.md")
        template = load_prompt(app_config.prompt_dir, "linkedin_search.prompt.md")
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"unable to load linkedin search template: {exc}")

    prompt = render_prompt(
        template,
        {
            "shared_restrictions": shared_restrictions,
            "row_context": json.dumps(row.model_dump(), indent=2),
            "extra_restrictions": json.dumps(restrictions, indent=2),
        },
    )
    try:
        text, usage = _run_with_retry(
            lambda: llm_client.complete(prompt),
            retries=int(app_config.non_resume_node_retries),
            backoff_seconds=float(app_config.non_resume_retry_backoff_seconds),
        )
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"linkedin target generation failed: {exc}")
    return _mark_succeeded(state, node, artifacts={"linkedin_targets_md": text}, usage=usage)


def outreach_node(state: GraphState) -> dict[str, Any]:
    node = "outreach_node"
    app_config = state.get("app_config")
    row = state.get("row_context")
    llm_client = state.get("llm_client")
    restrictions = state.get("restrictions", {})
    artifacts = dict(state.get("artifacts", {}))
    statuses = dict(state.get("node_status", {}))
    if statuses.get("linkedin_search_node") != "succeeded":
        return _mark_skipped(state, node, "linkedin targets unavailable")
    if app_config is None or row is None or llm_client is None:
        return _mark_failed(state, node, "missing graph context for outreach")

    try:
        shared_restrictions = load_prompt(app_config.prompt_dir, "shared_restrictions.prompt.md")
        template = load_prompt(app_config.prompt_dir, "outreach.prompt.md")
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"unable to load outreach template: {exc}")

    prompt = render_prompt(
        template,
        {
            "shared_restrictions": shared_restrictions,
            "row_context": json.dumps(row.model_dump(), indent=2),
            "linkedin_targets_md": artifacts.get("linkedin_targets_md", ""),
            "extra_restrictions": json.dumps(restrictions, indent=2),
        },
    )
    try:
        text, usage = _run_with_retry(
            lambda: llm_client.complete(prompt),
            retries=int(app_config.non_resume_node_retries),
            backoff_seconds=float(app_config.non_resume_retry_backoff_seconds),
        )
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"outreach generation failed: {exc}")

    linkedin_notes, cold_email = _split_outreach_output(text)
    return _mark_succeeded(
        state,
        node,
        artifacts={"linkedin_notes_md": linkedin_notes, "cold_email_md": cold_email},
        usage=usage,
    )


def render_node(state: GraphState) -> dict[str, Any]:
    node = "render_node"
    output_dir = state.get("output_dir")
    artifacts = dict(state.get("artifacts", {}))
    if not output_dir:
        return _mark_failed(state, node, "output_dir not set")
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)

    file_artifacts: dict[str, str] = {}
    try:
        if artifacts.get("resume_patch_md"):
            file_artifacts["resume_patch_md"] = str(
                write_markdown(base / "resume_patch.md", artifacts["resume_patch_md"]).name
            )
        if artifacts.get("resume_full_md"):
            resume_full_path = write_markdown(base / "resume_full.md", artifacts["resume_full_md"])
            file_artifacts["resume_full_md"] = str(resume_full_path.name)
            file_artifacts["resume_docx"] = str(write_docx(base / "resume.docx", artifacts["resume_full_md"]).name)
            file_artifacts["resume_pdf"] = str(write_pdf(base / "resume.pdf", artifacts["resume_full_md"]).name)
        if artifacts.get("cover_letter_md"):
            cover_path = write_markdown(base / "cover_letter.md", artifacts["cover_letter_md"])
            file_artifacts["cover_letter_md"] = str(cover_path.name)
            file_artifacts["cover_letter_docx"] = str(
                write_docx(base / "cover_letter.docx", artifacts["cover_letter_md"]).name
            )
            file_artifacts["cover_letter_pdf"] = str(
                write_pdf(base / "cover_letter.pdf", artifacts["cover_letter_md"]).name
            )
        if artifacts.get("linkedin_targets_md"):
            file_artifacts["linkedin_targets_md"] = str(
                write_markdown(base / "linkedin_targets.md", artifacts["linkedin_targets_md"]).name
            )
        if artifacts.get("linkedin_notes_md"):
            file_artifacts["linkedin_notes_md"] = str(
                write_markdown(base / "linkedin_connection_notes.md", artifacts["linkedin_notes_md"]).name
            )
        if artifacts.get("cold_email_md"):
            file_artifacts["cold_email_md"] = str(
                write_markdown(base / "cold_email.md", artifacts["cold_email_md"]).name
            )
    except Exception as exc:  # noqa: BLE001
        return _mark_failed(state, node, f"render failed: {exc}")
    return _mark_succeeded(state, node, artifacts=file_artifacts)


def report_node(state: GraphState) -> dict[str, Any]:
    node = "report_node"
    output_dir = state.get("output_dir")
    if not output_dir:
        return _mark_succeeded(state, node)
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    manifest = {
        "row_number": state.get("row_number"),
        "node_status": state.get("node_status", {}),
        "node_errors": state.get("node_errors", {}),
        "token_usage_by_node": state.get("token_usage_by_node", {}),
        "artifacts": state.get("artifacts", {}),
    }
    manifest_path = base / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    updates = _mark_succeeded(state, node)
    merged_artifacts = dict(updates.get("artifacts", {}))
    merged_artifacts["manifest_json"] = manifest_path.name
    updates["artifacts"] = merged_artifacts
    return updates


def _ensure_maps(
    state: GraphState,
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, dict[str, int]], dict[str, Any]]:
    node_status = dict(state.get("node_status", {}))
    node_errors = dict(state.get("node_errors", {}))
    token_usage_by_node = dict(state.get("token_usage_by_node", {}))
    artifacts = dict(state.get("artifacts", {}))
    return node_status, node_errors, token_usage_by_node, artifacts


def _mark_succeeded(
    state: GraphState,
    node: str,
    artifacts: dict[str, Any] | None = None,
    usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    node_status, node_errors, token_usage_by_node, merged_artifacts = _ensure_maps(state)
    node_status[node] = "succeeded"
    node_errors.setdefault(node, [])
    if artifacts:
        merged_artifacts.update(artifacts)
    if usage:
        token_usage_by_node[node] = {
            "input_tokens": int(usage.get("input_tokens", 0)),
            "output_tokens": int(usage.get("output_tokens", 0)),
            "total_tokens": int(usage.get("total_tokens", 0)),
        }
    return {
        "node_status": node_status,
        "node_errors": node_errors,
        "token_usage_by_node": token_usage_by_node,
        "artifacts": merged_artifacts,
    }


def _mark_failed(
    state: GraphState, node: str, error: str, usage: dict[str, int] | None = None
) -> dict[str, Any]:
    node_status, node_errors, token_usage_by_node, merged_artifacts = _ensure_maps(state)
    node_status[node] = "failed"
    node_errors.setdefault(node, []).append(error)
    if usage:
        token_usage_by_node[node] = {
            "input_tokens": int(usage.get("input_tokens", 0)),
            "output_tokens": int(usage.get("output_tokens", 0)),
            "total_tokens": int(usage.get("total_tokens", 0)),
        }
    return {
        "node_status": node_status,
        "node_errors": node_errors,
        "token_usage_by_node": token_usage_by_node,
        "artifacts": merged_artifacts,
    }


def _mark_skipped(state: GraphState, node: str, reason: str) -> dict[str, Any]:
    node_status, node_errors, token_usage_by_node, merged_artifacts = _ensure_maps(state)
    node_status[node] = "skipped"
    node_errors.setdefault(node, []).append(reason)
    return {
        "node_status": node_status,
        "node_errors": node_errors,
        "token_usage_by_node": token_usage_by_node,
        "artifacts": merged_artifacts,
    }


def _load_yaml_file(path: str | Path) -> dict[str, Any]:
    import yaml

    file_path = Path(path)
    if not file_path.exists():
        return {}
    with file_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    return data


def _run_with_retry(
    fn: Callable[[], tuple[str, dict[str, int]]],
    retries: int,
    backoff_seconds: float,
) -> tuple[str, dict[str, int]]:
    attempts = max(0, retries) + 1
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == attempts - 1:
                break
            time.sleep(max(0.0, backoff_seconds * (2**attempt)))
    assert last_error is not None
    raise last_error


def _extract_selected_ids(
    selection_text: str, resume_context: dict[str, Any]
) -> tuple[list[str], list[str]]:
    exp_ids = [str(item.get("id", "")).strip() for item in resume_context.get("experiences", []) if item.get("id")]
    proj_ids = [str(item.get("id", "")).strip() for item in resume_context.get("projects", []) if item.get("id")]
    lower = selection_text.lower()
    selected_exp = [item for item in exp_ids if item.lower() in lower]
    selected_proj = [item for item in proj_ids if item.lower() in lower]
    if not selected_exp:
        selected_exp = exp_ids[: min(3, len(exp_ids))]
    if not selected_proj:
        selected_proj = proj_ids[: min(2, len(proj_ids))]
    return list(dict.fromkeys(selected_exp)), list(dict.fromkeys(selected_proj))


def _split_context(
    resume_context: dict[str, Any],
    selected_experience_ids: list[str],
    selected_project_ids: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    experiences = resume_context.get("experiences", [])
    projects = resume_context.get("projects", [])
    selected_experiences = [e for e in experiences if str(e.get("id", "")) in selected_experience_ids]
    unselected_experiences = [e for e in experiences if str(e.get("id", "")) not in selected_experience_ids]
    selected_projects = [p for p in projects if str(p.get("id", "")) in selected_project_ids]
    unselected_projects = [p for p in projects if str(p.get("id", "")) not in selected_project_ids]
    selected_context = {
        "profile_summary": resume_context.get("profile_summary", ""),
        "skills": resume_context.get("skills", []),
        "experiences": selected_experiences,
        "projects": selected_projects,
    }
    unselected_context = {"experiences": unselected_experiences, "projects": unselected_projects}
    return selected_context, unselected_context


def _build_resume_patch_md(
    selected_experience_ids: list[str], selected_project_ids: list[str], rewritten_text: str
) -> str:
    return "\n".join(
        [
            "# Resume Patch",
            "",
            "## Selected Module IDs",
            f"- Experiences: {', '.join(selected_experience_ids) if selected_experience_ids else 'None'}",
            f"- Projects: {', '.join(selected_project_ids) if selected_project_ids else 'None'}",
            "",
            "## Rewritten Sections",
            rewritten_text.strip(),
            "",
        ]
    )


def _build_resume_full_md(
    resume_context: dict[str, Any],
    unselected_context: dict[str, Any],
    rewritten_text: str,
) -> str:
    lines = ["# Resume (Assembled)", "", "## Profile Summary", str(resume_context.get("profile_summary", "")), "", "## Skills"]
    for skill in resume_context.get("skills", []):
        lines.append(f"- {skill}")
    lines.extend(["", "## Selected + Rewritten Modules", rewritten_text.strip(), "", "## Unselected Experiences (Unchanged)"])
    for exp in unselected_context.get("experiences", []):
        lines.append(f"### {exp.get('company', '')} - {exp.get('role', '')} ({exp.get('id', '')})")
        for bullet in exp.get("bullets", []):
            lines.append(f"- {bullet}")
    lines.extend(["", "## Unselected Projects (Unchanged)"])
    for project in unselected_context.get("projects", []):
        lines.append(f"### {project.get('name', '')} ({project.get('id', '')})")
        for bullet in project.get("bullets", []):
            lines.append(f"- {bullet}")
    return "\n".join(lines).strip() + "\n"


def _split_outreach_output(text: str) -> tuple[str, str]:
    marker_1 = "## LinkedIn Connection Notes"
    marker_2 = "## Cold Email"
    if marker_1 in text and marker_2 in text:
        left, right = text.split(marker_2, 1)
        linkedin = left.strip()
        cold = (marker_2 + "\n" + right.strip()).strip()
        return linkedin + "\n", cold + "\n"
    return text.strip() + "\n", text.strip() + "\n"

