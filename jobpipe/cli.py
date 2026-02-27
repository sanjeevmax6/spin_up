from __future__ import annotations

import logging
from pathlib import Path

import typer

from jobpipe.config import AppConfig, load_env_settings
from jobpipe.graph import execute_graph_pipeline
from jobpipe.llm import build_llm_client
from jobpipe.rules import load_rules
from jobpipe.sheets import GoogleSheetsClient
from jobpipe.utils.rows import parse_rows_csv

app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.command()
def prep(
    rows: str = typer.Option(
        ...,
        "--rows",
        help="Comma-separated IDs (Sno if row_lookup_mode=sno, else sheet row numbers)",
    ),
    sheet_id: str | None = typer.Option(None, "--sheet-id", help="Google Sheet ID override"),
    tab: str | None = typer.Option(None, "--tab", help="Sheet tab override"),
    model: str | None = typer.Option(None, "--model", help="Model override"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Use deterministic dry-run LLM outputs"),
    output_dir: str | None = typer.Option(None, "--output-dir", help="Output directory override"),
    config_path: str = typer.Option("config.yaml", "--config", help="Path to config yaml"),
    rules_path: str = typer.Option("rules.yaml", "--rules", help="Path to rules yaml"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level (DEBUG, INFO, WARNING, ERROR)"),
) -> None:
    numeric_level = getattr(logging, str(log_level).upper(), None)
    if not isinstance(numeric_level, int):
        raise typer.BadParameter("Invalid --log-level. Use DEBUG, INFO, WARNING, or ERROR.")
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    app_cfg = AppConfig.load(config_path)
    if sheet_id:
        app_cfg.sheet_id = sheet_id
    if tab:
        app_cfg.tab = tab
    if model:
        app_cfg.model = model
    if output_dir:
        app_cfg.output_root = output_dir

    env = load_env_settings()
    if not dry_run and not env.openai_api_key:
        raise typer.BadParameter("OPENAI_API_KEY is required unless --dry-run is enabled")
    if not env.google_service_account_json:
        raise typer.BadParameter("GOOGLE_SERVICE_ACCOUNT_JSON is required")
    if not app_cfg.sheet_id:
        raise typer.BadParameter("sheet_id must be set in config.yaml or via --sheet-id")

    row_numbers = parse_rows_csv(rows)
    if not row_numbers:
        raise typer.BadParameter("No row numbers provided")

    sheets_client = GoogleSheetsClient(env.google_service_account_json)
    rules = load_rules(rules_path)
    llm_client = build_llm_client(env.openai_api_key, app_cfg.model, dry_run=dry_run)

    results, run_dir = execute_graph_pipeline(
        rows=row_numbers,
        app_config=app_cfg,
        rule_set=rules,
        sheets_client=sheets_client,
        llm_client=llm_client,
        output_dir=Path(app_cfg.output_root),
        dry_run=dry_run,
    )

    succeeded = [r for r in results if r.status == "succeeded"]
    failed = [r for r in results if r.status != "succeeded"]

    typer.echo(f"Run dir: {run_dir}")
    typer.echo(f"Succeeded: {len(succeeded)}")
    typer.echo(f"Failed: {len(failed)}")
    for item in failed:
        typer.echo(f"- Row {item.row_number}: {'; '.join(item.errors)}")

    raise typer.Exit(code=1 if failed else 0)
