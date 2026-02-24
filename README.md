# jobpipe

Local Python CLI for job application personalization:
- reads selected Google Sheet rows by `Sno` (or literal sheet row number)
- validates rows and hard rules
- generates resume bullet bank, cover letter, and LinkedIn outreach
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
- share your sheet with the service account email

## Command

```bash
jobpipe prep --rows 12,15,22
```

If `config.yaml` has `row_lookup_mode: "sno"`, those values are treated as `Sno` IDs.
If `row_lookup_mode: "sheet_row"`, they are treated as actual sheet row numbers.
For custom layouts, you can force where `Sno` lives:
- `row_id_column_index: 3` means `Sno` is in column C (1-based index)
- `header_row_number: <n>` can force which row is used as headers

Optional overrides:

```bash
jobpipe prep --rows 12,15,22 --sheet-id <id> --tab Sheet1 --model gpt-4.1-mini --dry-run --output-dir outputs
```

Exit codes:
- `0` all rows succeeded
- `1` one or more rows failed

## Output

`outputs/<timestamp>/<row>-<company>-<role>/` contains:
- `resume_bullet_bank.md`
- `cover_letter.md`
- `linkedin_outreach.md`
- `cover_letter.docx`
- `cover_letter.pdf`
- `bundle.pdf`
- `manifest.json`

Run-level:
- `outputs/<timestamp>/run_report.json`
