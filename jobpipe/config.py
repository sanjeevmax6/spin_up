from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseModel):
    sheet_id: str = ""
    tab: str = "Sheet1"
    output_root: str = "outputs"
    model: str = "gpt-4.1-mini"
    row_lookup_mode: str = "sheet_row"
    row_id_column: str = "sno"
    row_id_column_index: int | None = None
    header_row_number: int | None = None
    max_scan_rows: int = 5000
    header_search_rows: int = 50
    column_aliases: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "row_id": ["sno", "serial no", "id"],
            "company": ["companies", "company"],
            "role_title": ["type", "role_title", "title"],
            "job_posting_url": ["roles", "job_posting_url", "job link", "url"],
            "job_description_url": ["roles", "job_description_url", "jd_url"],
            "job_description_text": ["job_description_text", "job description"],
            "location": ["location"],
            "notes": ["notes", "referrals"],
            "priority": ["priority", "keywords"],
            "status": [
                "sanjeev - date/applied(color)",
                "reach out status",
                "swetha - date/applied(color)",
                "status",
            ],
            "recruiter_name": ["linkedin recruiters connect", "recruiter_name"],
            "keywords": ["keywords"],
            "employment_type": ["full time/intern", "fulltime/internship"],
        }
    )
    baseline_profile: str = (
        "Candidate profile not provided yet. Keep wording factual and avoid assumptions."
    )

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        file_path = Path(path)
        if not file_path.exists():
            return cls()
        with file_path.open("r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}
        return cls(**data)


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    google_service_account_json: str = Field(
        default="", alias="GOOGLE_SERVICE_ACCOUNT_JSON"
    )


def load_env_settings() -> EnvSettings:
    load_dotenv()
    return EnvSettings()
