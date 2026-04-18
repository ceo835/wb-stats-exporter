"""Google Sheets storage service for positions collector."""

from __future__ import annotations

import logging
import os
import hashlib
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from google.auth.exceptions import TransportError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .positions_groups import POSITION_CATEGORY_ORDER, classify_position_category
from .positions_models import CollectorState, PositionPair, PositionRecord

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_API_MAX_RETRIES = 5
GOOGLE_API_RETRY_BASE_SLEEP_SECONDS = 2.0

DEFAULT_SETTINGS_SHEET = "Настройки"
DEFAULT_RAW_SHEET = "Positions_Raw"
DEFAULT_STATE_SHEET = "Positions_State"
DEFAULT_MATRIX_INDEX_SHEET = "Positions_Matrix_Index"
DEFAULT_MATRIX_SHEET_PREFIX = "Matrix"
DEFAULT_CATEGORY_INDEX_SHEET = "Positions_Category_Index"
DEFAULT_CATEGORY_SHEET_PREFIX = "Category"

STATE_HEADERS = ["key", "value", "updated_at"]
RAW_HEADERS = [
    "date",
    "collected_at",
    "nm_id",
    "product_name",
    "user_query",
    "matched_query",
    "match_type",
    "position",
    "organic_position",
    "boost_position",
    "traffic_volume",
    "status",
    "data_source",
    "error_msg",
]
SETTINGS_HEADERS = ["nm_id", "query", "product_name", "is_own_brand"]
MATRIX_INDEX_HEADERS = ["query", "sheet_name", "nm_count", "rows", "date_from", "date_to", "updated_at"]
CATEGORY_INDEX_HEADERS = ["category", "sheet_name", "nm_count", "rows", "date_from", "date_to", "updated_at"]
MOJIBAKE_SHEET_ALIASES = {
    "РќР°СЃС‚СЂРѕР№РєРё": "Настройки",
    "Р СњР В°РЎРѓРЎвЂљРЎР‚Р С•Р в„–Р С”Р С‘": "Настройки",
}


class PositionsSheetsService:
    """Read/write settings, state, and positions data in Google Sheets."""

    def __init__(
        self,
        credentials_file: str,
        spreadsheet_id: str,
        logger: logging.Logger,
        settings_sheet: str = DEFAULT_SETTINGS_SHEET,
        raw_sheet: str = DEFAULT_RAW_SHEET,
        state_sheet: str = DEFAULT_STATE_SHEET,
    ):
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id
        self.logger = logger
        self.settings_sheet = settings_sheet
        self.raw_sheet = raw_sheet
        self.state_sheet = state_sheet
        self.service = self._build_service()

    @classmethod
    def from_env(cls, logger: logging.Logger) -> "PositionsSheetsService":
        """Create service from environment variables."""
        credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
        spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "").strip()
        if not credentials_file or not spreadsheet_id:
            raise RuntimeError("GOOGLE_CREDENTIALS_FILE or GOOGLE_SPREADSHEET_ID is not configured.")
        settings_sheet = cls._normalize_sheet_name(
            os.getenv("POSITIONS_SETTINGS_SHEET", DEFAULT_SETTINGS_SHEET),
            DEFAULT_SETTINGS_SHEET,
        )
        raw_sheet = cls._normalize_sheet_name(
            os.getenv("POSITIONS_RAW_SHEET", DEFAULT_RAW_SHEET),
            DEFAULT_RAW_SHEET,
        )
        state_sheet = cls._normalize_sheet_name(
            os.getenv("POSITIONS_STATE_SHEET", DEFAULT_STATE_SHEET),
            DEFAULT_STATE_SHEET,
        )
        return cls(
            credentials_file=credentials_file,
            spreadsheet_id=spreadsheet_id,
            logger=logger,
            settings_sheet=settings_sheet,
            raw_sheet=raw_sheet,
            state_sheet=state_sheet,
        )

    def ensure_base_sheets(self) -> None:
        """Ensure state and raw sheets exist with headers."""
        self._ensure_sheet_with_headers(self.raw_sheet, RAW_HEADERS)
        self._ensure_sheet_with_headers(self.state_sheet, STATE_HEADERS)

    def load_pairs(self, csv_path: str = "") -> list[PositionPair]:
        """Load (nm_id, query) pairs from CSV or Settings sheet."""
        if csv_path:
            csv_file = Path(csv_path)
            if csv_file.is_file():
                pairs = self._load_pairs_from_df(pd.read_csv(csv_file), source_label="settings_csv")
                self.logger.info("Loaded positions settings from CSV: %s (pairs=%d)", csv_file, len(pairs))
                return pairs

        values = self._read_values(self.settings_sheet)
        if not values:
            self.logger.warning("Settings sheet %s is empty.", self.settings_sheet)
            return []

        header = values[0]
        rows = values[1:]
        if not rows:
            return []

        frame = pd.DataFrame(rows, columns=header)
        pairs = self._load_pairs_from_df(frame, source_label="settings_sheet")
        self.logger.info("Loaded positions settings from sheet %s (pairs=%d)", self.settings_sheet, len(pairs))
        return pairs

    def load_positions_raw(self) -> pd.DataFrame:
        """Read raw positions sheet."""
        self._ensure_sheet_with_headers(self.raw_sheet, RAW_HEADERS)
        values = self._read_values(self.raw_sheet)
        if len(values) <= 1:
            return pd.DataFrame(columns=RAW_HEADERS)

        header = values[0]
        rows = values[1:]
        normalized_rows: list[list[Any]] = []
        header_len = len(header)
        for row in rows:
            current = list(row)
            if len(current) < header_len:
                current.extend([""] * (header_len - len(current)))
            elif len(current) > header_len:
                current = current[:header_len]
            normalized_rows.append(current)

        frame = pd.DataFrame(normalized_rows, columns=header)
        frame = self._migrate_raw_frame(frame)
        return frame

    def upsert_positions(self, records: list[PositionRecord]) -> int:
        """Upsert records in raw sheet by key (date, nm_id, user_query)."""
        if not records:
            return 0

        self._ensure_sheet_with_headers(self.raw_sheet, RAW_HEADERS)
        existing = self.load_positions_raw()

        new_frame = pd.DataFrame([record.as_dict() for record in records])
        for column in RAW_HEADERS:
            if column not in new_frame.columns:
                new_frame[column] = pd.NA
        new_frame = new_frame[RAW_HEADERS]
        new_frame["date"] = new_frame["date"].fillna("").astype(str).str.strip()
        new_frame["collected_at"] = new_frame["collected_at"].fillna("").astype(str).str.strip()
        new_frame["nm_id"] = pd.to_numeric(new_frame["nm_id"], errors="coerce").astype("Int64")
        new_frame["product_name"] = new_frame["product_name"].fillna("").astype(str).str.strip()
        new_frame["user_query"] = new_frame["user_query"].fillna("").astype(str).str.strip()
        new_frame["matched_query"] = new_frame["matched_query"].fillna("").astype(str).str.strip()
        new_frame["match_type"] = new_frame["match_type"].fillna("").astype(str).str.strip()
        new_frame["position"] = pd.to_numeric(new_frame["position"], errors="coerce").astype("Int64")
        new_frame["organic_position"] = pd.to_numeric(new_frame["organic_position"], errors="coerce").astype("Int64")
        new_frame["boost_position"] = pd.to_numeric(new_frame["boost_position"], errors="coerce").astype("Int64")
        new_frame["traffic_volume"] = pd.to_numeric(new_frame["traffic_volume"], errors="coerce").astype("Int64")
        new_frame["status"] = new_frame["status"].fillna("").astype(str).str.strip().str.lower()
        new_frame["data_source"] = new_frame["data_source"].fillna("").astype(str).str.strip().str.lower()
        new_frame["error_msg"] = new_frame["error_msg"].fillna("").astype(str).str.strip()
        new_frame.loc[new_frame["status"] == "", "status"] = "not_found"
        new_frame.loc[new_frame["data_source"] == "", "data_source"] = "unknown"
        new_frame = new_frame.dropna(subset=["nm_id"])

        key_existing = (
            existing["date"].astype(str)
            + "|"
            + existing["nm_id"].astype(str)
            + "|"
            + existing["user_query"].astype(str).str.strip().str.lower()
        )
        key_new = (
            new_frame["date"].astype(str)
            + "|"
            + new_frame["nm_id"].astype(str)
            + "|"
            + new_frame["user_query"].astype(str).str.strip().str.lower()
        )

        existing = existing.assign(_key=key_existing)
        new_frame = new_frame.assign(_key=key_new)

        existing = existing.drop_duplicates(subset="_key", keep="last")
        new_frame = new_frame.drop_duplicates(subset="_key", keep="last")

        existing = existing[~existing["_key"].isin(new_frame["_key"])].copy()
        merged = pd.concat(
            [existing.drop(columns=["_key"]), new_frame.drop(columns=["_key"])],
            ignore_index=True,
        )
        merged = merged.sort_values(by=["date", "nm_id", "user_query"], ascending=[True, True, True]).reset_index(
            drop=True
        )

        values = [RAW_HEADERS]
        values.extend(self._frame_to_values(merged, RAW_HEADERS))
        self._write_values(self.raw_sheet, values)
        return len(new_frame)

    def refresh_query_matrix_sheets(self, start_date: str = "") -> int:
        """Build/update matrix sheets per query from raw positions data."""
        raw = self.load_positions_raw()
        if raw.empty:
            return 0

        frame = raw.copy()
        frame["date"] = frame["date"].fillna("").astype(str).str.strip()
        frame["user_query"] = frame["user_query"].fillna("").astype(str).str.strip()
        frame["collected_at"] = frame["collected_at"].fillna("").astype(str).str.strip()
        frame["nm_id"] = pd.to_numeric(frame["nm_id"], errors="coerce").astype("Int64")
        frame["organic_position"] = pd.to_numeric(frame["organic_position"], errors="coerce").astype("Int64")
        frame["boost_position"] = pd.to_numeric(frame["boost_position"], errors="coerce").astype("Int64")

        frame = frame.dropna(subset=["nm_id"]).copy()
        frame = frame[(frame["date"] != "") & (frame["user_query"] != "")].copy()
        if start_date:
            frame = frame[frame["date"] >= start_date].copy()
        if frame.empty:
            return 0

        # Keep latest measurement per (date, query, nm_id).
        latest = (
            frame.sort_values(by=["date", "collected_at"])
            .groupby(["date", "user_query", "nm_id"], as_index=False)
            .tail(1)
            .copy()
        )
        if latest.empty:
            return 0

        prefix = os.getenv("POSITIONS_MATRIX_SHEET_PREFIX", DEFAULT_MATRIX_SHEET_PREFIX).strip()
        if not prefix:
            prefix = DEFAULT_MATRIX_SHEET_PREFIX
        index_sheet = os.getenv("POSITIONS_MATRIX_INDEX_SHEET", DEFAULT_MATRIX_INDEX_SHEET).strip()
        if not index_sheet:
            index_sheet = DEFAULT_MATRIX_INDEX_SHEET

        updated_count = 0
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        index_rows: list[dict[str, Any]] = []

        for query in sorted(latest["user_query"].dropna().astype(str).unique().tolist()):
            query_text = str(query).strip()
            if not query_text:
                continue

            query_frame = latest[latest["user_query"] == query_text].copy()
            matrix = self._build_matrix_for_query(query_frame)
            if matrix.empty:
                continue

            sheet_name = self._matrix_sheet_title(prefix=prefix, query=query_text)
            self._ensure_sheet_exists(sheet_name)

            matrix_headers = list(matrix.columns)
            values = [matrix_headers]
            values.extend(self._frame_to_values(matrix, matrix_headers))
            self._write_values(sheet_name, values)

            sheet_id = self._sheet_id_by_title(sheet_name)
            if sheet_id is not None:
                self._format_matrix_sheet(
                    sheet_id=sheet_id,
                    rows_count=len(matrix) + 1,
                    cols_count=len(matrix_headers),
                    is_data_matrix=True,
                )

            date_values = matrix["Дата"].astype(str).tolist()
            index_rows.append(
                {
                    "query": query_text,
                    "sheet_name": sheet_name,
                    "nm_count": int(query_frame["nm_id"].nunique()),
                    "rows": len(matrix),
                    "date_from": date_values[0] if date_values else "",
                    "date_to": date_values[-1] if date_values else "",
                    "updated_at": updated_at,
                }
            )
            updated_count += 1

        index_frame = pd.DataFrame(index_rows, columns=MATRIX_INDEX_HEADERS)
        if not index_frame.empty:
            index_frame = index_frame.sort_values(by=["query", "sheet_name"]).reset_index(drop=True)
            self._ensure_sheet_with_headers(index_sheet, MATRIX_INDEX_HEADERS)
            index_values = [MATRIX_INDEX_HEADERS]
            index_values.extend(self._frame_to_values(index_frame, MATRIX_INDEX_HEADERS))
            self._write_values(index_sheet, index_values)

            index_sheet_id = self._sheet_id_by_title(index_sheet)
            if index_sheet_id is not None:
                self._format_matrix_sheet(
                    sheet_id=index_sheet_id,
                    rows_count=len(index_frame) + 1,
                    cols_count=len(MATRIX_INDEX_HEADERS),
                    is_data_matrix=False,
                )
        return updated_count

    def refresh_category_matrix_sheets(self, start_date: str = "") -> int:
        """Build/update matrix sheets per category from raw positions data."""
        raw = self.load_positions_raw()
        if raw.empty:
            return 0

        frame = raw.copy()
        frame["date"] = frame["date"].fillna("").astype(str).str.strip()
        frame["collected_at"] = frame["collected_at"].fillna("").astype(str).str.strip()
        frame["nm_id"] = pd.to_numeric(frame["nm_id"], errors="coerce").astype("Int64")
        frame["organic_position"] = pd.to_numeric(frame["organic_position"], errors="coerce").astype("Int64")
        frame["boost_position"] = pd.to_numeric(frame["boost_position"], errors="coerce").astype("Int64")
        if "product_name" in frame.columns:
            frame["product_name"] = frame["product_name"].fillna("").astype(str).str.strip()
        else:
            frame["product_name"] = ""
        if "user_query" in frame.columns:
            frame["user_query"] = frame["user_query"].fillna("").astype(str).str.strip()
        else:
            frame["user_query"] = ""
        if "matched_query" in frame.columns:
            frame["matched_query"] = frame["matched_query"].fillna("").astype(str).str.strip()
        else:
            frame["matched_query"] = ""

        frame = frame.dropna(subset=["nm_id"]).copy()
        frame = frame[frame["date"] != ""].copy()
        if start_date:
            frame = frame[frame["date"] >= start_date].copy()
        if frame.empty:
            return 0

        frame["category"] = frame.apply(
            lambda row: classify_position_category(
                row.get("product_name", ""),
                row.get("user_query", ""),
                row.get("matched_query", ""),
            ),
            axis=1,
        )
        frame = frame[frame["category"].isin(POSITION_CATEGORY_ORDER)].copy()
        if frame.empty:
            return 0

        prefix = os.getenv("POSITIONS_CATEGORY_SHEET_PREFIX", DEFAULT_CATEGORY_SHEET_PREFIX).strip()
        if not prefix:
            prefix = DEFAULT_CATEGORY_SHEET_PREFIX
        index_sheet = os.getenv("POSITIONS_CATEGORY_INDEX_SHEET", DEFAULT_CATEGORY_INDEX_SHEET).strip()
        if not index_sheet:
            index_sheet = DEFAULT_CATEGORY_INDEX_SHEET

        updated_count = 0
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        index_rows: list[dict[str, Any]] = []

        for category in POSITION_CATEGORY_ORDER:
            category_frame = frame[frame["category"] == category].copy()
            if category_frame.empty:
                latest = pd.DataFrame(columns=["date", "nm_id", "organic_position", "boost_position"])
                matrix = pd.DataFrame(columns=["Дата"])
            else:
                latest = (
                    category_frame.sort_values(by=["date", "collected_at"])
                    .groupby(["date", "nm_id"], as_index=False)
                    .tail(1)
                    .copy()
                )
                matrix = self._build_matrix_for_query(latest)
                if matrix.empty:
                    matrix = pd.DataFrame(columns=["Дата"])

            sheet_name = self._matrix_sheet_title(prefix=prefix, query=category)
            self._ensure_sheet_exists(sheet_name)

            matrix_headers = list(matrix.columns)
            values = [matrix_headers]
            values.extend(self._frame_to_values(matrix, matrix_headers))
            self._write_values(sheet_name, values)

            sheet_id = self._sheet_id_by_title(sheet_name)
            if sheet_id is not None:
                self._format_matrix_sheet(
                    sheet_id=sheet_id,
                    rows_count=max(1, len(matrix) + 1),
                    cols_count=len(matrix_headers),
                    is_data_matrix=True,
                )

            date_values = matrix["Дата"].astype(str).tolist() if "Дата" in matrix.columns else []
            index_rows.append(
                {
                    "category": category,
                    "sheet_name": sheet_name,
                    "nm_count": int(latest["nm_id"].nunique()),
                    "rows": len(matrix),
                    "date_from": date_values[0] if date_values else "",
                    "date_to": date_values[-1] if date_values else "",
                    "updated_at": updated_at,
                }
            )
            updated_count += 1

        index_frame = pd.DataFrame(index_rows, columns=CATEGORY_INDEX_HEADERS)
        if not index_frame.empty:
            index_frame = index_frame.sort_values(by=["category", "sheet_name"]).reset_index(drop=True)
            self._ensure_sheet_with_headers(index_sheet, CATEGORY_INDEX_HEADERS)
            index_values = [CATEGORY_INDEX_HEADERS]
            index_values.extend(self._frame_to_values(index_frame, CATEGORY_INDEX_HEADERS))
            self._write_values(index_sheet, index_values)

            index_sheet_id = self._sheet_id_by_title(index_sheet)
            if index_sheet_id is not None:
                self._format_matrix_sheet(
                    sheet_id=index_sheet_id,
                    rows_count=len(index_frame) + 1,
                    cols_count=len(CATEGORY_INDEX_HEADERS),
                    is_data_matrix=False,
                )
        return updated_count

    def _build_matrix_for_query(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Build date x (nm_id, metric) matrix for one query."""
        if frame.empty:
            return pd.DataFrame(columns=["Дата"])

        source = frame.copy()
        source["date"] = source["date"].fillna("").astype(str).str.strip()
        source = source[source["date"] != ""].copy()
        if source.empty:
            return pd.DataFrame(columns=["Дата"])

        source["nm_id"] = pd.to_numeric(source["nm_id"], errors="coerce").astype("Int64")
        source = source.dropna(subset=["nm_id"]).copy()
        if source.empty:
            return pd.DataFrame(columns=["Дата"])

        source["nm_id"] = source["nm_id"].astype(int)
        source["organic_position"] = pd.to_numeric(source["organic_position"], errors="coerce")
        source["boost_position"] = pd.to_numeric(source["boost_position"], errors="coerce")

        date_list = sorted(source["date"].astype(str).unique().tolist())
        matrix = pd.DataFrame({"Дата": date_list})
        organic_wide = source.pivot_table(index="date", columns="nm_id", values="organic_position", aggfunc="last")
        boost_wide = source.pivot_table(index="date", columns="nm_id", values="boost_position", aggfunc="last")

        all_nm = sorted(set(organic_wide.columns.tolist()) | set(boost_wide.columns.tolist()))
        for nm_id in all_nm:
            organic_col = organic_wide.get(nm_id, pd.Series(index=date_list, dtype="float64"))
            boost_col = boost_wide.get(nm_id, pd.Series(index=date_list, dtype="float64"))
            matrix[f"{nm_id} | Органика"] = matrix["Дата"].map(organic_col.to_dict())
            matrix[f"{nm_id} | Буст"] = matrix["Дата"].map(boost_col.to_dict())

        for column in matrix.columns:
            if column == "Дата":
                continue
            matrix[column] = pd.to_numeric(matrix[column], errors="coerce").astype("Int64")

        matrix["Дата"] = pd.to_datetime(matrix["Дата"], errors="coerce").dt.strftime("%d.%m.%Y").fillna(matrix["Дата"])
        return matrix

    def _matrix_sheet_title(self, prefix: str, query: str) -> str:
        """Create deterministic safe title for query matrix sheet."""
        prefix_text = self._sanitize_sheet_fragment(prefix) or DEFAULT_MATRIX_SHEET_PREFIX
        query_text = self._sanitize_sheet_fragment(query) or "query"
        suffix = hashlib.md5(query.encode("utf-8")).hexdigest()[:6]

        max_title_len = 100
        fixed_len = len(prefix_text) + len(suffix) + 2  # "<prefix> <query>_<hash>"
        max_query_len = max(12, max_title_len - fixed_len)
        short_query = query_text[:max_query_len].strip()
        title = f"{prefix_text} {short_query}_{suffix}".strip()
        return title[:max_title_len]

    @staticmethod
    def _sanitize_sheet_fragment(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"[:\\\\/?*\\[\\]]", " ", text)
        text = " ".join(text.split())
        return text

    def get_collector_state(self) -> CollectorState:
        """Load collector state from state sheet."""
        self._ensure_sheet_with_headers(self.state_sheet, STATE_HEADERS)
        values = self._read_values(self.state_sheet)
        mapping: dict[str, str] = {}
        for row in values[1:]:
            if not row:
                continue
            key = str(row[0]).strip()
            if not key:
                continue
            value = str(row[1]).strip() if len(row) > 1 else ""
            mapping[key] = value
        return CollectorState.from_mapping(mapping)

    def set_collector_state(self, updates: dict[str, str], updated_at: str) -> CollectorState:
        """Upsert collector state keys."""
        self._ensure_sheet_with_headers(self.state_sheet, STATE_HEADERS)
        state = self.get_collector_state().as_mapping()
        for key, value in updates.items():
            state[str(key)] = str(value)

        values = [STATE_HEADERS]
        for key in sorted(state.keys()):
            values.append([key, state[key], updated_at])
        self._write_values(self.state_sheet, values)
        return CollectorState.from_mapping(state)

    def request_trigger(self, requested_at: str) -> CollectorState:
        """Set trigger_pending flag for background collector."""
        return self.set_collector_state(
            updates={
                "trigger_pending": "1",
                "trigger_requested_at": requested_at,
            },
            updated_at=requested_at,
        )

    def mark_running(self, run_time: str) -> CollectorState:
        """Mark collector as running and consume trigger."""
        return self.set_collector_state(
            updates={
                "running": "1",
                "trigger_pending": "0",
                "last_run_status": "running",
                "last_run_time": run_time,
                "last_error": "",
            },
            updated_at=run_time,
        )

    def mark_success(self, run_time: str, rows: int) -> CollectorState:
        """Mark collector success."""
        return self.set_collector_state(
            updates={
                "running": "0",
                "trigger_pending": "0",
                "last_run_status": "success",
                "last_run_time": run_time,
                "last_error": "",
                "last_run_rows": str(rows),
            },
            updated_at=run_time,
        )

    def mark_error(self, run_time: str, error_message: str) -> CollectorState:
        """Mark collector failure."""
        return self.set_collector_state(
            updates={
                "running": "0",
                "trigger_pending": "0",
                "last_run_status": "error",
                "last_run_time": run_time,
                "last_error": error_message[:500],
            },
            updated_at=run_time,
        )

    def _build_service(self) -> Any:
        if not self.credentials_file or not self.spreadsheet_id:
            raise RuntimeError("Google Sheets credentials or spreadsheet id is empty.")
        if not os.path.isfile(self.credentials_file):
            raise FileNotFoundError(f"Credentials file not found: {self.credentials_file}")
        credentials = service_account.Credentials.from_service_account_file(
            self.credentials_file,
            scopes=SCOPES,
        )
        return build("sheets", "v4", credentials=credentials, cache_discovery=False)

    def _execute_request(self, request: Any, operation: str) -> Any:
        """Execute Google API request with retry for quota and transient errors."""
        attempt = 0
        while True:
            attempt += 1
            try:
                return request.execute()
            except HttpError as exc:
                status = getattr(getattr(exc, "resp", None), "status", None)
                if attempt >= GOOGLE_API_MAX_RETRIES or not self._is_retryable_http_error(status):
                    raise
                sleep_seconds = GOOGLE_API_RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1))
                self.logger.warning(
                    "Google Sheets %s failed with HTTP %s (attempt %d/%d), retry in %.1f sec.",
                    operation,
                    status,
                    attempt,
                    GOOGLE_API_MAX_RETRIES,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
            except (TransportError, OSError) as exc:
                if attempt >= GOOGLE_API_MAX_RETRIES:
                    raise
                sleep_seconds = GOOGLE_API_RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1))
                self.logger.warning(
                    "Google Sheets %s failed with network error %s (attempt %d/%d), retry in %.1f sec.",
                    operation,
                    exc,
                    attempt,
                    GOOGLE_API_MAX_RETRIES,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

    @staticmethod
    def _is_retryable_http_error(status: Optional[int]) -> bool:
        if status is None:
            return False
        return int(status) in {429, 500, 502, 503, 504}

    def _sheet_titles(self) -> set[str]:
        metadata = self._execute_request(
            self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id),
            operation="spreadsheets.get",
        )
        return {
            sheet.get("properties", {}).get("title", "")
            for sheet in metadata.get("sheets", [])
        }

    def _sheet_properties_map(self) -> dict[str, dict[str, Any]]:
        metadata = self._execute_request(
            self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id),
            operation="spreadsheets.get",
        )
        result: dict[str, dict[str, Any]] = {}
        for sheet in metadata.get("sheets", []):
            props = sheet.get("properties", {})
            title = str(props.get("title", "")).strip()
            if title:
                result[title] = props
        return result

    def _sheet_id_by_title(self, title: str) -> Optional[int]:
        props = self._sheet_properties_map().get(title)
        if not props:
            return None
        value = props.get("sheetId")
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _ensure_sheet_exists(self, title: str) -> None:
        if title not in self._sheet_titles():
            self._create_sheet(title)

    def _create_sheet(self, title: str) -> None:
        self._execute_request(
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
            ),
            operation="spreadsheets.batchUpdate:addSheet",
        )

    def _ensure_sheet_with_headers(self, title: str, headers: list[str]) -> None:
        titles = self._sheet_titles()
        if title not in titles:
            self._create_sheet(title)
            self._write_values(title, [headers])
            return

        existing = self._read_values(title)
        if not existing:
            self._write_values(title, [headers])
            return
        first_row = [str(cell).strip() for cell in existing[0]]
        if first_row != headers:
            if title == self.raw_sheet:
                normalized_rows: list[list[Any]] = []
                header_len = len(first_row)
                for row in existing[1:]:
                    current = list(row)
                    if len(current) < header_len:
                        current.extend([""] * (header_len - len(current)))
                    elif len(current) > header_len:
                        current = current[:header_len]
                    normalized_rows.append(current)
                old_frame = pd.DataFrame(normalized_rows, columns=first_row)
                migrated = self._migrate_raw_frame(old_frame)
                values = [RAW_HEADERS]
                values.extend(self._frame_to_values(migrated, RAW_HEADERS))
                self._write_values(title, values)
                return

            values = [headers]
            values.extend(existing[1:])
            self._write_values(title, values)

    def _migrate_raw_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize legacy raw frames to current RAW_HEADERS schema."""
        raw = frame.copy()
        columns_lower = {str(column).strip().lower(): column for column in raw.columns}

        def get_series(*names: str, default: str = "") -> pd.Series:
            for name in names:
                column = columns_lower.get(name.lower())
                if column is not None:
                    return raw[column]
            return pd.Series([default] * len(raw))

        migrated = pd.DataFrame(
            {
                "date": get_series("date"),
                "collected_at": get_series("collected_at"),
                "nm_id": get_series("nm_id"),
                "product_name": get_series("product_name", "name", default=""),
                "user_query": get_series("user_query", "query", default=""),
                "matched_query": get_series("matched_query", default=""),
                "match_type": get_series("match_type", default=""),
                "position": get_series("position"),
                "organic_position": get_series("organic_position", default=""),
                "boost_position": get_series("boost_position", "position", default=""),
                "traffic_volume": get_series("traffic_volume", default=""),
                "status": get_series("status", default=""),
                "data_source": get_series("data_source", "source", default=""),
                "error_msg": get_series("error_msg", "error", default=""),
            }
        )

        migrated["date"] = migrated["date"].fillna("").astype(str).str.strip()
        migrated["collected_at"] = migrated["collected_at"].fillna("").astype(str).str.strip()
        migrated["nm_id"] = pd.to_numeric(migrated["nm_id"], errors="coerce").astype("Int64")
        migrated["product_name"] = migrated["product_name"].fillna("").astype(str).str.strip()
        migrated["user_query"] = migrated["user_query"].fillna("").astype(str).str.strip()
        migrated["matched_query"] = migrated["matched_query"].fillna("").astype(str).str.strip()
        migrated.loc[migrated["matched_query"] == "", "matched_query"] = migrated.loc[
            migrated["matched_query"] == "", "user_query"
        ]
        migrated["match_type"] = migrated["match_type"].fillna("").astype(str).str.strip().str.lower()
        missing_match_type = migrated["match_type"] == ""
        migrated.loc[missing_match_type, "match_type"] = "exact"
        migrated["position"] = pd.to_numeric(migrated["position"], errors="coerce").astype("Int64")
        migrated["organic_position"] = pd.to_numeric(migrated["organic_position"], errors="coerce").astype("Int64")
        migrated["boost_position"] = pd.to_numeric(migrated["boost_position"], errors="coerce").astype("Int64")
        migrated["traffic_volume"] = pd.to_numeric(migrated["traffic_volume"], errors="coerce").astype("Int64")
        migrated["status"] = migrated["status"].fillna("").astype(str).str.strip().str.lower()
        migrated["status"] = migrated["status"].apply(self._normalize_status)
        migrated["data_source"] = migrated["data_source"].fillna("").astype(str).str.strip().str.lower()
        migrated.loc[~migrated["data_source"].isin({"mpstats", "wb_analytics"}), "data_source"] = "unknown"
        migrated["error_msg"] = migrated["error_msg"].fillna("").astype(str).str.strip()
        migrated = migrated.dropna(subset=["nm_id"]).copy()
        return migrated[RAW_HEADERS]

    def _read_values(self, title: str) -> list[list[Any]]:
        response = self._execute_request(
            self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{title}!A1:Z",
            ),
            operation=f"values.get:{title}",
        )
        return response.get("values", [])

    def _write_values(self, title: str, values: list[list[Any]]) -> None:
        self._execute_request(
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"{title}!A:ZZZ",
            ),
            operation=f"values.clear:{title}",
        )
        self._execute_request(
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{title}!A1",
                valueInputOption="RAW",
                body={"values": values},
            ),
            operation=f"values.update:{title}",
        )

    def _format_matrix_sheet(self, sheet_id: int, rows_count: int, cols_count: int, is_data_matrix: bool) -> None:
        """Apply basic visual formatting for matrix-style sheets."""
        if rows_count <= 0 or cols_count <= 0:
            return

        requests: list[dict[str, Any]] = [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": cols_count,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.90, "green": 0.93, "blue": 0.98},
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": rows_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment)",
                }
            },
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": cols_count,
                    }
                }
            },
        ]

        if is_data_matrix and cols_count > 1:
            for column_index in range(1, cols_count):
                is_organic = (column_index - 1) % 2 == 0
                body_color = (
                    {"red": 0.93, "green": 0.96, "blue": 1.0}
                    if is_organic
                    else {"red": 0.92, "green": 0.98, "blue": 0.94}
                )
                header_color = (
                    {"red": 0.80, "green": 0.87, "blue": 0.98}
                    if is_organic
                    else {"red": 0.79, "green": 0.93, "blue": 0.82}
                )
                requests.extend(
                    [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1,
                                    "startColumnIndex": column_index,
                                    "endColumnIndex": column_index + 1,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": header_color,
                                        "textFormat": {"bold": True},
                                        "horizontalAlignment": "CENTER",
                                    }
                                },
                                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                            }
                        },
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 1,
                                    "endRowIndex": rows_count,
                                    "startColumnIndex": column_index,
                                    "endColumnIndex": column_index + 1,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": body_color,
                                        "horizontalAlignment": "CENTER",
                                        "numberFormat": {"type": "NUMBER", "pattern": "0"},
                                    }
                                },
                                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,numberFormat)",
                            }
                        },
                    ]
                )
        else:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": rows_count,
                            "startColumnIndex": 1,
                            "endColumnIndex": cols_count,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER",
                            }
                        },
                        "fields": "userEnteredFormat(horizontalAlignment)",
                    }
                }
            )

        self._execute_request(
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": requests},
            ),
            operation=f"spreadsheets.batchUpdate:format:{sheet_id}",
        )
    @staticmethod
    def _to_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if not text:
            return default
        return text in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _parse_id_set(raw_value: str) -> set[int]:
        value = str(raw_value or "").strip()
        if not value:
            return set()
        tokenized = value.replace(";", ",").replace("\n", ",").replace("\t", ",")
        result: set[int] = set()
        for chunk in tokenized.split(","):
            text = chunk.strip()
            if not text:
                continue
            try:
                result.add(int(text))
            except ValueError:
                continue
        return result

    @staticmethod
    def _normalize_status(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"found", "ok"}:
            return "found"
        if text in {"not_found", "not found"}:
            return "not_found"
        if text in {"source_error", "error", "fail", "failed"}:
            return "source_error"
        return "not_found"

    @staticmethod
    def _normalize_sheet_name(value: str, default: str) -> str:
        text = str(value or "").strip()
        if not text:
            return default
        return MOJIBAKE_SHEET_ALIASES.get(text, text)

    def _load_pairs_from_df(self, frame: pd.DataFrame, source_label: str) -> list[PositionPair]:
        columns_lower = {str(column).strip().lower(): column for column in frame.columns}
        if "nm_id" not in columns_lower:
            raise RuntimeError("Settings must contain column nm_id.")
        if "user_query" not in columns_lower and "query" not in columns_lower:
            raise RuntimeError("Settings must contain query or user_query column.")

        nm_col = columns_lower["nm_id"]
        query_col = columns_lower.get("user_query") or columns_lower.get("query")
        own_brand_col: Optional[Any] = columns_lower.get("is_own_brand")
        product_name_col: Optional[Any] = columns_lower.get("product_name")
        source_col: Optional[Any] = columns_lower.get("source")
        own_brand_ids = self._parse_id_set(os.getenv("MY_NM_IDS", "")) | self._parse_id_set(
            os.getenv("POSITIONS_MY_NM_IDS", "")
        )

        base = frame.copy()
        base[nm_col] = pd.to_numeric(base[nm_col], errors="coerce")
        base[query_col] = base[query_col].fillna("").astype(str).str.strip()
        base = base.dropna(subset=[nm_col])
        base = base[base[query_col] != ""]

        base[nm_col] = base[nm_col].astype(int)
        if own_brand_col is not None:
            base["__is_own_brand"] = base[own_brand_col].apply(lambda value: self._to_bool(value, default=False))
        else:
            base["__is_own_brand"] = False
        if own_brand_ids:
            base["__is_own_brand"] = base["__is_own_brand"] | base[nm_col].isin(own_brand_ids)

        if product_name_col is not None:
            base["__product_name"] = base[product_name_col].fillna("").astype(str).str.strip()
        else:
            base["__product_name"] = ""

        if source_col is not None:
            base["__source"] = base[source_col].fillna("").astype(str).str.strip()
            base.loc[base["__source"] == "", "__source"] = source_label
        else:
            base["__source"] = source_label

        base = base.drop_duplicates(subset=[nm_col, query_col]).reset_index(drop=True)
        return [
            PositionPair(
                nm_id=int(row[nm_col]),
                user_query=str(row[query_col]).strip(),
                product_name=str(row["__product_name"]).strip(),
                is_own_brand=bool(row["__is_own_brand"]),
                source=str(row["__source"]).strip() or source_label,
            )
            for _, row in base.iterrows()
        ]

    @staticmethod
    def _frame_to_values(frame: pd.DataFrame, columns: list[str]) -> list[list[Any]]:
        rows: list[list[Any]] = []
        for row in frame[columns].itertuples(index=False, name=None):
            values: list[Any] = []
            for value in row:
                if hasattr(value, "item"):
                    try:
                        value = value.item()
                    except Exception:
                        pass
                if value is None:
                    values.append("")
                    continue
                if isinstance(value, float) and pd.isna(value):
                    values.append("")
                    continue
                try:
                    if pd.isna(value):
                        values.append("")
                        continue
                except Exception:
                    pass
                if isinstance(value, (datetime, date)):
                    values.append(value.isoformat())
                    continue
                if isinstance(value, pd.Timestamp):
                    values.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                    continue
                if isinstance(value, bool):
                    values.append("1" if value else "0")
                    continue
                if isinstance(value, int):
                    values.append(int(value))
                    continue
                if isinstance(value, float):
                    values.append(float(value))
                    continue
                text_value = str(value)
                if text_value.lower() == "nan":
                    values.append("")
                    continue
                values.append(text_value)
            rows.append(values)
        return rows


