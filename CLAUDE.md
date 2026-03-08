# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (editable with dev deps)
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run the pipeline
jobpipe --rows 12,15,22

# With overrides
jobpipe --rows 12 --model gpt-4.1-mini --dry-run --log-level DEBUG

# Run tests
pytest
```

All commands should be run from the repo root with the venv activated.

## Environment

`.env` must contain:
- `OPENAI_API_KEY` — required unless `--dry-run`
- `GOOGLE_SERVICE_ACCOUNT_JSON` — always required (path to service account JSON file)

`config.yaml` must have `sheet_id` set. Share your Google Sheet with the service account email.

## Architecture

This is a **LangGraph-based job application personalization pipeline**. For each requested row in a Google Sheet, it runs a stateful graph that produces resume, cover letter, LinkedIn outreach, and cold email artifacts.

### Data flow

```
CLI (cli.py)
  → loads AppConfig, EnvSettings, RuleSet, GoogleSheetsClient, LLMClient
  → execute_graph_pipeline (graph.py)
      → builds LangGraph StateGraph (build_row_graph)
      → per row: ingest → validate → resume → [cover_letter || linkedin_search] → outreach → render → report
      → writes outputs/<timestamp>/<company>-<role>/
      → writes outputs/<timestamp>/run_report.json
```

### Key modules

| File | Role |
|---|---|
| `jobpipe/cli.py` | Typer entrypoint, wires config/env/clients |
| `jobpipe/graph.py` | LangGraph topology + `execute_graph_pipeline` loop |
| `jobpipe/agent.py` | All node implementations (ingest, validate, resume, cover, linkedin, outreach, render, report) |
| `jobpipe/state.py` | `GraphState` TypedDict with merge reducers for parallel fan-out |
| `jobpipe/models.py` | `JobRow`, `RuleSet`, `RowResult` Pydantic/dataclass types |
| `jobpipe/config.py` | `AppConfig` (from `config.yaml`) and `EnvSettings` (from `.env`) |
| `jobpipe/llm.py` | `LLMClient` (OpenAI Responses API) + `DryRunLLMClient` |
| `jobpipe/rules.py` | Loads `rules.yaml` and evaluates hard disqualifiers against row data |
| `jobpipe/sheets.py` | Google Sheets API client with flexible header/column aliasing |
| `jobpipe/utils/rendering.py` | Markdown, DOCX, PDF writers + `slugify` |
| `jobpipe/utils/rows.py` | Row parsing, required-column validation |
| `jobpipe/utils/prompting.py` | Prompt template load/render from `jobpipe/prompts/` |
| `jobpipe/pipeline.py` | Legacy non-graph pipeline (kept but superseded by `graph.py`) |

### State and parallelism

`GraphState` uses `Annotated` merge reducers (`merge_dict`, `merge_error_dict`) so LangGraph can merge partial state updates from parallel branches. After `resume_node`, `cover_letter_node` and `linkedin_search_node` run in parallel (both conditional from `resume_node`), then reconverge at `render_node`.

### Resume generation (JSON-first)

The resume pipeline is a multi-step LLM chain inside `resume_node`:
1. **selection** — picks experience/project IDs from `resume_context.yaml` relevant to the JD
2. **rewrite** — rewrites selected items as tight, quantified bullets
3. **trim** — enforces one-page policy by reducing projects (4→3→2) before cutting experiences

Profile summary, skills, and portfolio links are static (from the DOCX template); only experiences and projects are LLM-generated.

### Configuration files

- `config.yaml` — runtime settings (sheet ID, model, paths, column aliases, bullet word limits)
- `rules.yaml` — `hard_disqualifiers`, `style_constraints`, `banned_claims`, `must_include_constraints`
- `resume_context.yaml` — modular inventory of experiences and projects for the LLM to select from
- `restrictions.yaml` — global writing constraints injected into all prompts via `shared_restrictions.prompt.md`
- `templates/resume_template.docx` — DOCX Jinja2 template rendered by `docxtpl`

### Column aliasing

Sheet columns are mapped to `JobRow` fields via `column_aliases` in `config.yaml`. The `row_lookup_mode` setting controls whether `--rows` values are treated as `Sno` IDs (`sno`) or literal sheet row numbers (`sheet_row`).

### PDF conversion

`pdf_converter_command` in `config.yaml` is an optional shell command (e.g. LibreOffice). If unset, PDF output is skipped with a warning.
