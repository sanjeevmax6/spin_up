from __future__ import annotations

from typing import Any

import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel


class GoogleSheetsClient:
    def __init__(self, service_account_json: str) -> None:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = service_account.Credentials.from_service_account_file(
            service_account_json, scopes=scopes
        )
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def get_rows(
        self,
        sheet_id: str,
        tab: str,
        row_numbers: list[int],
        app_config: BaseModel,
    ) -> dict[int, dict[str, Any]]:
        if not row_numbers:
            return {}
        sorted_rows = sorted(set(row_numbers))
        range_name = f"{tab}!A1:ZZ{int(getattr(app_config, 'max_scan_rows', 5000))}"
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=range_name)
            .execute()
        )
        values = result.get("values", [])
        if not values:
            return {}
        header_row_number = getattr(app_config, "header_row_number", None)
        header_idx: int | None
        if isinstance(header_row_number, int) and header_row_number > 0:
            header_idx = header_row_number - 1
        else:
            header_idx = _detect_header_row(
                values=values,
                aliases=getattr(app_config, "column_aliases", {}),
                search_rows=int(getattr(app_config, "header_search_rows", 50)),
            )
        if header_idx is None:
            return {}
        if header_idx >= len(values):
            return {}
        headers = values[header_idx]
        mode = str(getattr(app_config, "row_lookup_mode", "sheet_row")).strip().lower()
        if mode == "sno":
            return _map_rows_by_id_column(
                values=values,
                header_idx=header_idx,
                headers=headers,
                target_ids=sorted_rows,
                aliases=getattr(app_config, "column_aliases", {}),
                id_field=str(getattr(app_config, "row_id_column", "sno")),
                id_column_index=getattr(app_config, "row_id_column_index", None),
            )
        return _map_rows_by_sheet_index(
            values=values,
            headers=headers,
            target_rows=sorted_rows,
            aliases=getattr(app_config, "column_aliases", {}),
        )


def _normalize_key(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _build_alias_map(aliases: dict[str, list[str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for canonical, alias_list in aliases.items():
        mapping[_normalize_key(canonical)] = canonical
        for alias in alias_list:
            mapping[_normalize_key(alias)] = canonical
    return mapping


def _detect_header_row(
    values: list[list[str]], aliases: dict[str, list[str]], search_rows: int
) -> int | None:
    alias_map = _build_alias_map(aliases)
    best_idx: int | None = None
    best_score = -1
    for idx, row in enumerate(values[: max(1, search_rows)]):
        seen: set[str] = set()
        for cell in row:
            canonical = alias_map.get(_normalize_key(cell))
            if canonical:
                seen.add(canonical)
        score = len(seen)
        if score > best_score:
            best_score = score
            best_idx = idx
    if best_score < 3:
        return None
    return best_idx


def _raw_row_dict(headers: list[str], row_values: list[str]) -> dict[str, str]:
    return {
        headers[i]: row_values[i] if i < len(row_values) else ""
        for i in range(len(headers))
        if headers[i]
    }


def _canonicalize_row(raw: dict[str, Any], aliases: dict[str, list[str]]) -> dict[str, Any]:
    alias_map = _build_alias_map(aliases)
    canonical: dict[str, Any] = {}
    for raw_key, raw_value in raw.items():
        normalized = _normalize_key(str(raw_key))
        target = alias_map.get(normalized)
        if not target:
            continue
        if target not in canonical or not str(canonical[target]).strip():
            canonical[target] = raw_value

    # Useful fallbacks for your sheet format.
    role_value = str(canonical.get("role_title", "")).strip()
    roles_value = str(canonical.get("job_posting_url", "")).strip()
    if (not role_value or role_value.startswith("http")) and roles_value and not roles_value.startswith("http"):
        canonical["role_title"] = roles_value

    if roles_value.startswith("http"):
        canonical.setdefault("job_description_url", roles_value)

    if not str(canonical.get("status", "")).strip():
        status_bits = [
            str(raw.get("Sanjeev - Date/Applied(Color)", "")).strip(),
            str(raw.get("Reach Out Status", "")).strip(),
            str(raw.get("Swetha - Date/Applied(Color)", "")).strip(),
        ]
        canonical["status"] = " | ".join([b for b in status_bits if b])

    if not str(canonical.get("notes", "")).strip():
        note_bits = [
            str(raw.get("Referrals", "")).strip(),
            str(raw.get("Linkedin Recruiters Connect", "")).strip(),
            str(raw.get("Keywords", "")).strip(),
        ]
        canonical["notes"] = " | ".join([b for b in note_bits if b])

    if not str(canonical.get("priority", "")).strip():
        canonical["priority"] = "medium"

    if str(canonical.get("job_posting_url", "")).strip() and not str(canonical.get("job_description_url", "")).strip():
        canonical["job_description_url"] = str(canonical["job_posting_url"]).strip()

    if str(canonical.get("employment_type", "")).strip() and not str(canonical.get("work_mode", "")).strip():
        canonical["work_mode"] = str(canonical["employment_type"]).strip()

    return canonical


def _to_int_if_possible(value: Any) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _map_rows_by_sheet_index(
    values: list[list[str]],
    headers: list[str],
    target_rows: list[int],
    aliases: dict[str, list[str]],
) -> dict[int, dict[str, Any]]:
    mapped: dict[int, dict[str, Any]] = {}
    for row_num in target_rows:
        idx = row_num - 1
        if idx < 1 or idx >= len(values):
            continue
        raw = _raw_row_dict(headers, values[idx])
        mapped[row_num] = _canonicalize_row(raw, aliases)
    return mapped


def _map_rows_by_id_column(
    values: list[list[str]],
    header_idx: int,
    headers: list[str],
    target_ids: list[int],
    aliases: dict[str, list[str]],
    id_field: str,
    id_column_index: int | None = None,
) -> dict[int, dict[str, Any]]:
    target_set = set(target_ids)
    found: dict[int, dict[str, Any]] = {}
    alias_map = _build_alias_map(aliases)
    id_idx = None
    if isinstance(id_column_index, int) and id_column_index > 0:
        id_idx = id_column_index - 1
    else:
        for i, header in enumerate(headers):
            normalized = _normalize_key(header)
            canonical = alias_map.get(normalized)
            if canonical == id_field or normalized == _normalize_key(id_field):
                id_idx = i
                break
    if id_idx is None:
        return found

    for idx in range(header_idx + 1, len(values)):
        row_values = values[idx]
        row_id = _to_int_if_possible(row_values[id_idx] if id_idx < len(row_values) else "")
        if row_id is None or row_id not in target_set:
            continue
        raw = _raw_row_dict(headers, row_values)
        found[row_id] = _canonicalize_row(raw, aliases)
        if len(found) == len(target_set):
            break
    return found


def fetch_job_description(url: str, timeout_seconds: int = 12) -> str:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    text = " ".join(soup.get_text(separator=" ").split())
    return text[:16000]
