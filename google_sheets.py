"""Optional Google Sheets export for Streamlit app."""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from data_processor import (
    RAW_DISPLAY_COLUMNS,
    RAW_RENAME_MAP,
    ROW_TYPE_DISPLAY_MAP,
    SUMMARY_COLUMNS,
    SUMMARY_RENAME_MAP,
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LOG_SHEET_NAME = "Р›РѕРіРё"
SHEETS_WRITE_CHUNK_SIZE = 2000
ADS_EXPORT_SHEET_PREFIX_ENV = "ADS_GOOGLE_EXPORT_PREFIX"
DEFAULT_ADS_EXPORT_SHEET_PREFIX = "AdsStats"


@dataclass
class GoogleSheetsConfig:
    """Google Sheets configuration loaded from environment."""

    credentials_file: str
    spreadsheet_id: str

    @property
    def enabled(self) -> bool:
        return bool(self.credentials_file and self.spreadsheet_id)


def load_google_config() -> GoogleSheetsConfig:
    """Read Google Sheets configuration from env."""
    return GoogleSheetsConfig(
        credentials_file=os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip(),
        spreadsheet_id=os.getenv("GOOGLE_SPREADSHEET_ID", "").strip(),
    )


def is_google_sheets_configured() -> bool:
    """Quick check if Google Sheets integration is enabled."""
    config = load_google_config()
    return config.enabled and os.path.isfile(config.credentials_file)


class GoogleSheetsExporter:
    """Exporter that writes report data into Google Sheets."""

    def __init__(self, config: GoogleSheetsConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.service = self._build_service()

    def save_report(
        self,
        raw_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> str:
        """Create new date-range sheet and write report data."""
        if not self.config.enabled:
            raise RuntimeError("Google Sheets integration is not configured.")

        base_title = self._report_sheet_base_title(start_date=start_date, end_date=end_date)
        sheet_title = self._make_unique_sheet_title(base_title)
        self._create_sheet(sheet_title)

        raw_export, summary_export = self._prepare_export_frames(raw_df, summary_df)
        values: list[list[Any]] = []

        values.append(["Р”Р°РЅРЅС‹Рµ (СЃС‹СЂС‹Рµ)"])
        values.append([str(col) for col in raw_export.columns.tolist()])
        values.extend(
            [[self._serialize(cell) for cell in row] for row in raw_export.itertuples(index=False, name=None)]
        )

        values.append([])
        values.append(["РЎРІРѕРґРЅС‹Рµ"])
        values.append([str(col) for col in summary_export.columns.tolist()])
        values.extend(
            [[self._serialize(cell) for cell in row] for row in summary_export.itertuples(index=False, name=None)]
        )

        self._write_values_chunked(sheet_title=sheet_title, values=values)

        self.logger.info("Р”Р°РЅРЅС‹Рµ СЃРѕС…СЂР°РЅРµРЅС‹ РІ Google Sheets, Р»РёСЃС‚: %s", sheet_title)
        return sheet_title

    def _write_values_chunked(
        self,
        sheet_title: str,
        values: list[list[Any]],
        chunk_size: int = SHEETS_WRITE_CHUNK_SIZE,
    ) -> None:
        """Write values to Google Sheets in row chunks to avoid payload limits."""
        if not values:
            return

        start_row = 1
        for index in range(0, len(values), max(chunk_size, 1)):
            chunk = values[index : index + max(chunk_size, 1)]
            self.service.spreadsheets().values().update(
                spreadsheetId=self.config.spreadsheet_id,
                range=f"{sheet_title}!A{start_row}",
                valueInputOption="RAW",
                body={"values": chunk},
            ).execute()
            start_row += len(chunk)

    @staticmethod
    def _prepare_export_frames(
        raw_df: pd.DataFrame,
        summary_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Reorder and rename report columns to Russian labels."""
        raw_export = raw_df.copy()
        if raw_export.empty:
            raw_export = pd.DataFrame(columns=RAW_DISPLAY_COLUMNS)
        preferred_raw = [column for column in RAW_DISPLAY_COLUMNS if column in raw_export.columns]
        extra_raw = [column for column in raw_export.columns if column not in preferred_raw]
        raw_export = raw_export[preferred_raw + extra_raw].rename(columns=RAW_RENAME_MAP)
        if "РўРёРї СЃС‚СЂРѕРєРё" in raw_export.columns:
            raw_export["РўРёРї СЃС‚СЂРѕРєРё"] = raw_export["РўРёРї СЃС‚СЂРѕРєРё"].replace(ROW_TYPE_DISPLAY_MAP)

        summary_export = summary_df.copy()
        if summary_export.empty:
            summary_export = pd.DataFrame(columns=SUMMARY_COLUMNS)
        preferred_summary = [column for column in SUMMARY_COLUMNS if column in summary_export.columns]
        extra_summary = [column for column in summary_export.columns if column not in preferred_summary]
        summary_export = summary_export[preferred_summary + extra_summary].rename(columns=SUMMARY_RENAME_MAP)

        return raw_export, summary_export

    def append_run_log(
        self,
        status: str,
        rows_count: int,
        start_date: str,
        end_date: str,
        message: str = "",
        sheet_title: str = "",
    ) -> None:
        """Append metadata row into `Р›РѕРіРё` sheet."""
        self._ensure_log_sheet()
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        self.service.spreadsheets().values().append(
            spreadsheetId=self.config.spreadsheet_id,
            range=f"{LOG_SHEET_NAME}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={
                "values": [
                    [now, start_date, end_date, status, rows_count, sheet_title, message]
                ]
            },
        ).execute()

    def _build_service(self) -> Any:
        if not self.config.enabled:
            raise RuntimeError("Google Sheets config is missing.")
        if not os.path.isfile(self.config.credentials_file):
            raise FileNotFoundError(
                f"GOOGLE_CREDENTIALS_FILE not found: {self.config.credentials_file}"
            )

        credentials = service_account.Credentials.from_service_account_file(
            self.config.credentials_file, scopes=SCOPES
        )
        return build("sheets", "v4", credentials=credentials, cache_discovery=False)

    def _ensure_log_sheet(self) -> None:
        metadata = self._spreadsheet_metadata()
        titles = self._sheet_titles(metadata)
        if LOG_SHEET_NAME not in titles:
            self._create_sheet(LOG_SHEET_NAME)
            self.service.spreadsheets().values().update(
                spreadsheetId=self.config.spreadsheet_id,
                range=f"{LOG_SHEET_NAME}!A1:G1",
                valueInputOption="RAW",
                body={
                    "values": [
                        [
                            "run_utc",
                            "start_date",
                            "end_date",
                            "status",
                            "rows",
                            "sheet_title",
                            "message",
                        ]
                    ]
                },
            ).execute()

    def _make_unique_sheet_title(self, base_title: str) -> str:
        metadata = self._spreadsheet_metadata()
        titles = self._sheet_titles(metadata)
        if base_title not in titles:
            return base_title

        suffix = datetime.utcnow().strftime("%H%M%S")
        alt_title = f"{base_title}_{suffix}"
        if alt_title not in titles:
            return alt_title

        number = 1
        while f"{alt_title}_{number}" in titles:
            number += 1
        return f"{alt_title}_{number}"
    @staticmethod
    def _report_sheet_base_title(start_date: str, end_date: str) -> str:
        prefix = os.getenv(ADS_EXPORT_SHEET_PREFIX_ENV, DEFAULT_ADS_EXPORT_SHEET_PREFIX).strip()
        base_title = f"{start_date}_{end_date}"
        if not prefix:
            return base_title
        return f"{prefix}_{base_title}"

    def _spreadsheet_metadata(self) -> dict[str, Any]:
        return self.service.spreadsheets().get(
            spreadsheetId=self.config.spreadsheet_id
        ).execute()

    @staticmethod
    def _sheet_titles(metadata: dict[str, Any]) -> set[str]:
        return {
            sheet.get("properties", {}).get("title", "")
            for sheet in metadata.get("sheets", [])
        }

    def _create_sheet(self, title: str) -> None:
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.config.spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()

    @staticmethod
    def _serialize(value: Any) -> Any:
        if value is None:
            return ""

        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)

        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if hasattr(value, "item"):
            try:
                value = value.item()
            except Exception:
                pass

        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return ""

        return value


def try_save_to_google_sheets(
    raw_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> tuple[bool, str]:
    """Save report to Google Sheets if configured. Returns status and message."""
    config = load_google_config()
    if not config.enabled:
        return False, "GOOGLE_CREDENTIALS_FILE РёР»Рё GOOGLE_SPREADSHEET_ID РЅРµ Р·Р°РїРѕР»РЅРµРЅС‹."

    try:
        exporter = GoogleSheetsExporter(config=config, logger=logger)
        sheet_title = exporter.save_report(raw_df, summary_df, start_date, end_date)
        exporter.append_run_log(
            status="success",
            rows_count=len(raw_df),
            start_date=start_date,
            end_date=end_date,
            sheet_title=sheet_title,
            message="source=ads_statistics; export saved successfully.",
        )
        return True, f"Данные сохранены в лист: {sheet_title}"
    except (HttpError, OSError, RuntimeError) as exc:
        logger.warning("РћС€РёР±РєР° РІС‹РіСЂСѓР·РєРё РІ Google Sheets: %s", exc)
        try:
            config = load_google_config()
            if config.enabled:
                exporter = GoogleSheetsExporter(config=config, logger=logger)
                exporter.append_run_log(
                    status="error",
                    rows_count=len(raw_df),
                    start_date=start_date,
                    end_date=end_date,
                    message=f"source=ads_statistics; error={exc}",
                )
        except Exception:
            logger.warning("РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїРёСЃР°С‚СЊ РѕС€РёР±РєСѓ РІ Р»РёСЃС‚ Р›РѕРіРё.")
        return False, str(exc)


