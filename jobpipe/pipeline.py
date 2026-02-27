from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from jobpipe.config import AppConfig
from jobpipe.models import JobRow, RowResult
from jobpipe.prompt_builders import (
    build_cover_letter_prompt,
    build_linkedin_prompt,
    build_resume_prompt,
)
from jobpipe.rules import evaluate_row_rules
from jobpipe.sheets import fetch_job_description
from jobpipe.utils.rendering import slugify, write_docx, write_markdown, write_pdf
from jobpipe.utils.rows import validate_required_columns


def build_row_output_dir(run_dir: Path, row: JobRow) -> Path:
    folder_name = f"{slugify(row.company)}-{slugify(row.role_title)}"
    return run_dir / folder_name


def execute_pipeline(
    rows: list[int],
    app_config: AppConfig,
    rule_set: Any,
    sheets_client: Any,
    llm_client: Any,
    output_dir: str | Path | None = None,
    dry_run: bool = False,
) -> tuple[list[RowResult], Path]:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path(output_dir or app_config.output_root) / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = sheets_client.get_rows(
        app_config.sheet_id, app_config.tab, rows, app_config
    )

    results: list[RowResult] = []
    for row_number in rows:
        token_usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        errors: list[str] = []
        output_path: Path | None = None

        raw_row = raw_rows.get(row_number)
        if raw_row is None:
            results.append(
                RowResult(
                    row_number=row_number,
                    status="failed",
                    output_dir=None,
                    errors=["row not found in sheet based on lookup mode/header mapping"],
                    token_usage=token_usage,
                )
            )
            continue

        missing_columns = validate_required_columns(raw_row)
        if missing_columns:
            results.append(
                RowResult(
                    row_number=row_number,
                    status="failed",
                    output_dir=None,
                    errors=[f"missing required columns: {', '.join(missing_columns)}"],
                    token_usage=token_usage,
                )
            )
            continue

        try:
            row = JobRow.from_sheet_row(row_number=row_number, raw=raw_row)
        except ValidationError as e:
            results.append(
                RowResult(
                    row_number=row_number,
                    status="failed",
                    output_dir=None,
                    errors=[f"validation error: {err['msg']}" for err in e.errors()],
                    token_usage=token_usage,
                )
            )
            continue

        if not row.job_description_text and row.job_description_url:
            try:
                row.job_description_text = fetch_job_description(row.job_description_url)
            except Exception as e:  # noqa: BLE001
                results.append(
                    RowResult(
                        row_number=row_number,
                        status="failed",
                        output_dir=None,
                        errors=[f"unable to fetch job description URL: {e}"],
                        token_usage=token_usage,
                    )
                )
                continue

        rule_errors = evaluate_row_rules(row, rule_set)
        if rule_errors:
            results.append(
                RowResult(
                    row_number=row_number,
                    status="failed",
                    output_dir=None,
                    errors=rule_errors,
                    token_usage=token_usage,
                )
            )
            continue

        output_path = build_row_output_dir(run_dir, row)
        output_path.mkdir(parents=True, exist_ok=True)

        resume_prompt = build_resume_prompt(row, rule_set, app_config.baseline_profile)
        cover_prompt = build_cover_letter_prompt(row, rule_set, app_config.baseline_profile)
        linkedin_prompt = build_linkedin_prompt(row, rule_set, app_config.baseline_profile)

        try:
            resume_md, usage_1 = llm_client.complete(resume_prompt)
            cover_md, usage_2 = llm_client.complete(cover_prompt)
            linkedin_md, usage_3 = llm_client.complete(linkedin_prompt)
        except Exception as e:  # noqa: BLE001
            results.append(
                RowResult(
                    row_number=row_number,
                    status="failed",
                    output_dir=output_path,
                    errors=[f"generation error: {e}"],
                    token_usage=token_usage,
                )
            )
            continue

        for usage in (usage_1, usage_2, usage_3):
            for k in token_usage:
                token_usage[k] += int(usage.get(k, 0))

        resume_md_path = write_markdown(output_path / "resume_bullet_bank.md", resume_md)
        cover_md_path = write_markdown(output_path / "cover_letter.md", cover_md)
        linkedin_md_path = write_markdown(output_path / "linkedin_outreach.md", linkedin_md)
        cover_docx_path = write_docx(output_path / "cover_letter.docx", cover_md)
        cover_pdf_path = write_pdf(output_path / "cover_letter.pdf", cover_md)
        bundle_text = (
            "# Resume Bullets\n\n"
            + resume_md
            + "\n\n# Cover Letter\n\n"
            + cover_md
            + "\n\n# LinkedIn Outreach\n\n"
            + linkedin_md
        )
        bundle_pdf_path = write_pdf(output_path / "bundle.pdf", bundle_text)

        manifest = {
            "row_number": row_number,
            "company": row.company,
            "role_title": row.role_title,
            "dry_run": dry_run,
            "artifacts": {
                "resume_bullets_md": str(resume_md_path.name),
                "cover_letter_md": str(cover_md_path.name),
                "linkedin_targets_md": str(linkedin_md_path.name),
                "cover_letter_docx": str(cover_docx_path.name),
                "cover_letter_pdf": str(cover_pdf_path.name),
                "bundle_pdf": str(bundle_pdf_path.name),
            },
            "token_usage": token_usage,
        }
        (output_path / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )

        results.append(
            RowResult(
                row_number=row_number,
                status="succeeded",
                output_dir=output_path,
                errors=[],
                token_usage=token_usage,
            )
        )

    run_report = {
        "timestamp": timestamp,
        "total_requested": len(rows),
        "succeeded": len([r for r in results if r.status == "succeeded"]),
        "failed": len([r for r in results if r.status != "succeeded"]),
        "results": [asdict(r) | {"output_dir": str(r.output_dir) if r.output_dir else None} for r in results],
    }
    (run_dir / "run_report.json").write_text(json.dumps(run_report, indent=2), encoding="utf-8")
    return results, run_dir
