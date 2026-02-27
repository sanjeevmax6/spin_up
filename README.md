# jobpipe

Local Python CLI for job application personalization:
- reads selected Google Sheet rows by `Sno` (or literal sheet row number)
- runs a LangGraph workflow per row (resume -> parallel cover/linkedin -> outreach)
- validates rows, resume context, and hard rules
- generates JSON-first selected/refined resume artifacts, then renders resume from a DOCX template
- writes markdown, DOCX, PDF, and run metadata

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Update:
- `config.yaml` with your `sheet_id`
- `.env` with `OPENAI_API_KEY` and `GOOGLE_SERVICE_ACCOUNT_JSON`
- `resume_context.yaml` with your modular experience/project inventory
- `restrictions.yaml` with global writing constraints
- share your sheet with the service account email

## Command

```bash
jobpipe --rows 12,15,22
```

If `config.yaml` has `row_lookup_mode: "sno"`, those values are treated as `Sno` IDs.
If `row_lookup_mode: "sheet_row"`, they are treated as actual sheet row numbers.
For custom layouts, you can force where `Sno` lives:
- `row_id_column_index: 3` means `Sno` is in column C (1-based index)
- `header_row_number: <n>` can force which row is used as headers

Optional overrides:

```bash
jobpipe --rows 12,15,22 --sheet-id <id> --tab Sheet1 --model gpt-4.1-mini --dry-run --output-dir outputs
```

Exit codes:
- `0` all rows succeeded
- `1` one or more rows failed

## Output

`outputs/<timestamp>/<company>-<role>/` contains:
- `selected_ids.json`
- `refined_resume.json`
- `trimmed_resume.json`
- `resume.docx`
- `resume.pdf` (best effort; warning logged if no converter configured)
- `cover_letter.md`
- `linkedin_targets.md`
- `linkedin_connection_notes.md`
- `cold_email.md`
- `cover_letter.docx`
- `cover_letter.pdf`
- `manifest.json`

Run-level:
- `outputs/<timestamp>/run_report.json`

Notes:
- Profile summary/portfolio links and skills are treated as static template content.
- Dynamic generation focuses on experiences/projects only.
- One-page policy trims projects first (4 -> 3 -> 2) before touching experiences.

## Code Layout

- [cli.py](/Users/sanjeev/spin_up/jobpipe/cli.py): CLI entrypoint and config/env wiring
- [graph.py](/Users/sanjeev/spin_up/jobpipe/graph.py): LangGraph topology + per-row orchestration
- [agent.py](/Users/sanjeev/spin_up/jobpipe/agent.py): all node implementations (ingest/validate/resume/cover/linkedin/outreach/render/report)
- `jobpipe/utils/`
  - [rows.py](/Users/sanjeev/spin_up/jobpipe/utils/rows.py): row parsing + required-column validation
  - [prompting.py](/Users/sanjeev/spin_up/jobpipe/utils/prompting.py): prompt template load/render helpers
  - [rendering.py](/Users/sanjeev/spin_up/jobpipe/utils/rendering.py): markdown/docx/pdf writers and slugify
- [config.py](/Users/sanjeev/spin_up/jobpipe/config.py), [models.py](/Users/sanjeev/spin_up/jobpipe/models.py), [state.py](/Users/sanjeev/spin_up/jobpipe/state.py): core types/state
