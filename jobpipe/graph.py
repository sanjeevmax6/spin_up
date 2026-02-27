from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from langgraph.graph import END, START, StateGraph

from jobpipe import agent
from jobpipe.models import RowResult
from jobpipe.utils.rendering import slugify
from jobpipe.state import GraphState

logger = logging.getLogger(__name__)


def _route_after_ingest(state: GraphState) -> str:
    return "ok" if state.get("node_status", {}).get("ingest_row_node") == "succeeded" else "report"


def _route_after_validate(state: GraphState) -> str:
    return "ok" if state.get("node_status", {}).get("validate_context_node") == "succeeded" else "report"


def _route_after_resume(state: GraphState) -> str:
    return "ok" if state.get("node_status", {}).get("resume_node") == "succeeded" else "report"


def _route_after_resume_cover(state: GraphState) -> str:
    return _route_after_resume(state)


def _route_after_resume_linkedin(state: GraphState) -> str:
    return _route_after_resume(state)


def build_row_graph() -> Any:
    graph = StateGraph(GraphState)
    graph.add_node("ingest_row_node", agent.ingest_row_node)
    graph.add_node("validate_context_node", agent.validate_context_node)
    graph.add_node("resume_node", agent.resume_node)
    graph.add_node("cover_letter_node", agent.cover_letter_node)
    graph.add_node("linkedin_search_node", agent.linkedin_search_node)
    graph.add_node("outreach_node", agent.outreach_node)
    graph.add_node("render_node", agent.render_node)
    graph.add_node("report_node", agent.report_node)

    graph.add_edge(START, "ingest_row_node")
    graph.add_conditional_edges(
        "ingest_row_node",
        _route_after_ingest,
        {"ok": "validate_context_node", "report": "report_node"},
    )
    graph.add_conditional_edges(
        "validate_context_node",
        _route_after_validate,
        {"ok": "resume_node", "report": "report_node"},
    )
    graph.add_conditional_edges(
        "resume_node",
        _route_after_resume_cover,
        {"ok": "cover_letter_node", "report": "report_node"},
    )
    graph.add_conditional_edges(
        "resume_node",
        _route_after_resume_linkedin,
        {"ok": "linkedin_search_node", "report": "report_node"},
    )
    graph.add_edge("linkedin_search_node", "outreach_node")
    graph.add_edge("cover_letter_node", "render_node")
    graph.add_edge("outreach_node", "render_node")
    graph.add_edge("render_node", "report_node")
    graph.add_edge("report_node", END)
    return graph.compile()


def execute_graph_pipeline(
    rows: list[int],
    app_config: Any,
    rule_set: Any,
    sheets_client: Any,
    llm_client: Any,
    output_dir: str | Path | None = None,
    dry_run: bool = False,
) -> tuple[list[RowResult], Path]:
    logger.info("starting graph pipeline rows=%s dry_run=%s", rows, dry_run)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path(output_dir or app_config.output_root) / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = sheets_client.get_rows(app_config.sheet_id, app_config.tab, rows, app_config)
    logger.info("loaded sheet rows found=%s requested=%s", len(raw_rows), len(rows))
    graph = build_row_graph()

    results: list[RowResult] = []
    for row_number in rows:
        logger.info("processing row=%s", row_number)
        raw_row = raw_rows.get(row_number)
        if raw_row:
            company = str(raw_row.get("company", "row")).strip() or "row"
            role = str(raw_row.get("role_title", str(row_number))).strip() or str(row_number)
            row_dir = run_dir / f"{slugify(company)}-{slugify(role)}"
        else:
            row_dir = run_dir / f"row-{row_number}"

        initial_state: GraphState = {
            "row_number": row_number,
            "raw_row": raw_row or {},
            "rules": rule_set,
            "artifacts": {},
            "node_status": {},
            "node_errors": {},
            "token_usage_by_node": {},
            "output_dir": str(row_dir),
            "app_config": app_config,
            "llm_client": llm_client,
            "dry_run": dry_run,
        }
        final_state = graph.invoke(initial_state)

        statuses = dict(final_state.get("node_status", {}))
        node_errors = dict(final_state.get("node_errors", {}))
        flat_errors: list[str] = []
        for node, errs in node_errors.items():
            for err in errs:
                flat_errors.append(f"{node}: {err}")
        token_usage = _sum_usage(final_state.get("token_usage_by_node", {}))
        status = "succeeded" if _is_row_success(statuses) else "failed"
        logger.info(
            "row=%s completed status=%s node_status=%s",
            row_number,
            status,
            statuses,
        )

        results.append(
            RowResult(
                row_number=row_number,
                status=status,
                output_dir=row_dir if row_dir.exists() else None,
                errors=flat_errors,
                token_usage=token_usage,
            )
        )

    run_report = {
        "timestamp": timestamp,
        "total_requested": len(rows),
        "succeeded": len([r for r in results if r.status == "succeeded"]),
        "failed": len([r for r in results if r.status != "succeeded"]),
        "results": [
            asdict(r) | {"output_dir": str(r.output_dir) if r.output_dir else None}
            for r in results
        ],
    }
    (run_dir / "run_report.json").write_text(json.dumps(run_report, indent=2), encoding="utf-8")
    logger.info("run finished path=%s succeeded=%s failed=%s", run_dir, run_report["succeeded"], run_report["failed"])
    return results, run_dir


def _is_row_success(statuses: dict[str, str]) -> bool:
    required = ["ingest_row_node", "validate_context_node", "resume_node", "render_node", "report_node"]
    for key in required:
        if statuses.get(key) != "succeeded":
            return False
    return True


def _sum_usage(by_node: dict[str, dict[str, int]]) -> dict[str, int]:
    total = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    for usage in by_node.values():
        for key in total:
            total[key] += int(usage.get(key, 0))
    return total
