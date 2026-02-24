from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator


REQUIRED_COLUMNS = {
    "company",
    "role_title",
    "location",
    "job_posting_url",
}


class JobRow(BaseModel):
    row_number: int
    company: str
    role_title: str
    job_description_text: str | None = None
    job_description_url: str | None = None
    job_posting_url: str | None = None
    location: str
    work_mode: str | None = None
    seniority: str | None = None
    salary_range: str | None = None
    must_haves: str | None = None
    nice_to_haves: str | None = None
    application_deadline: str | None = None
    hiring_manager_name: str | None = None
    recruiter_name: str | None = None
    notes: str = ""
    priority: str = "medium"
    status: str = "new"

    @model_validator(mode="after")
    def validate_description_source(self) -> "JobRow":
        required_values = {
            "company": self.company,
            "role_title": self.role_title,
            "location": self.location,
        }
        missing_values = [k for k, v in required_values.items() if not str(v).strip()]
        if missing_values:
            raise ValueError(
                f"missing required field values: {', '.join(sorted(missing_values))}"
            )
        if not self.job_description_text and not self.job_description_url:
            raise ValueError(
                "One of 'job_description_text' or 'job_description_url' is required."
            )
        return self

    @classmethod
    def from_sheet_row(cls, row_number: int, raw: dict[str, Any]) -> "JobRow":
        cleaned = {}
        for key, value in raw.items():
            if key is None:
                continue
            normalized = key.strip().lower()
            cleaned[normalized] = value.strip() if isinstance(value, str) else value
        return cls(row_number=row_number, **cleaned)


class RuleSet(BaseModel):
    hard_disqualifiers: list[str] = Field(default_factory=list)
    style_constraints: list[str] = Field(default_factory=list)
    banned_claims: list[str] = Field(default_factory=list)
    must_include_constraints: list[str] = Field(default_factory=list)


class GeneratedArtifacts(BaseModel):
    resume_bullets_md: str
    cover_letter_md: str
    linkedin_targets_md: str
    cover_letter_docx: Path
    cover_letter_pdf: Path
    bundle_pdf: Path | None = None


@dataclass
class RowResult:
    row_number: int
    status: str
    output_dir: Path | None
    errors: list[str]
    token_usage: dict[str, int]
