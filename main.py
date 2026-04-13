"""Export Wildberries ad campaign statistics to Google Sheets."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

WB_BASE_URL = "https://advert-api.wildberries.ru"
MAX_BATCH_SIZE = 50
REQUEST_DELAY_SECONDS = 30
RATE_LIMIT_SLEEP_SECONDS = 90
MAX_RATE_LIMIT_SLEEP_SECONDS = 600
MAX_429_RETRIES = 6
DEFAULT_SHEET_NAME = "Sheet1"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
STOPPED_LOOKBACK_DAYS = 30
CONVERSION_TYPE_MAP = {
    1: "Прямая",
    32: "Ассоциированная",
    64: "Мультикарточка",
}
REPORT_COLUMNS = [
    "id РК",
    "Название",
    "Номенклатура",
    "Затраты, RUB",
    "Заказов на сумму, RUB",
    "Показы",
    "Клики",
    "Добавлений в корзину",
    "Заказанные товары, шт.",
    "CTR(%)",
    "CR (%)",
    "CPM",
    "CPC",
    "CPO",
    "Отмены",
    "Средняя позиция",
    "Номер мультикарточки",
    "Тип конверсии",
    "report_date",
]


@dataclass
class Config:
    """Runtime configuration loaded from environment variables."""

    wb_token: str
    google_credentials_file: str
    google_spreadsheet_id: str
    target_date: str
    log_level: str
    google_sheet_name: str
    filter_zero_spend: bool


def setup_logging(log_level: str) -> logging.Logger:
    """Configure logging to file and console."""
    logs_dir = Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "app.log"

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def load_config() -> Config:
    """Load and validate script configuration from environment variables."""
    load_dotenv()

    log_level = os.getenv("LOG_LEVEL", "INFO").strip() or "INFO"

    required_vars = {
        "WB_TOKEN": os.getenv("WB_TOKEN", "").strip(),
        "GOOGLE_CREDENTIALS_FILE": os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip(),
        "GOOGLE_SPREADSHEET_ID": os.getenv("GOOGLE_SPREADSHEET_ID", "").strip(),
    }
    missing = [name for name, value in required_vars.items() if not value]
    if missing:
        raise ValueError(
            "Missing required environment variables: " + ", ".join(sorted(missing))
        )

    target_date = os.getenv("TARGET_DATE", "").strip()
    if not target_date:
        target_date = (date.today() - timedelta(days=1)).isoformat()
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("TARGET_DATE must be in YYYY-MM-DD format.") from exc

    credentials_file = required_vars["GOOGLE_CREDENTIALS_FILE"]
    if not Path(credentials_file).is_file():
        raise FileNotFoundError(
            f"GOOGLE_CREDENTIALS_FILE not found: {credentials_file}"
        )

    sheet_name = os.getenv("GOOGLE_SHEET_NAME", DEFAULT_SHEET_NAME).strip()
    if not sheet_name:
        sheet_name = DEFAULT_SHEET_NAME
    filter_zero_spend = os.getenv("FILTER_ZERO_SPEND", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    return Config(
        wb_token=required_vars["WB_TOKEN"],
        google_credentials_file=credentials_file,
        google_spreadsheet_id=required_vars["GOOGLE_SPREADSHEET_ID"],
        target_date=target_date,
        log_level=log_level,
        google_sheet_name=sheet_name,
        filter_zero_spend=filter_zero_spend,
    )


def build_wb_headers(token: str) -> dict[str, str]:
    """Build headers for Wildberries API requests."""
    return {"Authorization": token, "Content-Type": "application/json"}


def request_adverts(session: requests.Session, token: str, statuses: Optional[str] = "9") -> Any:
    """Request campaign list from WB adverts endpoint."""
    headers = build_wb_headers(token)
    params = {"statuses": statuses} if statuses else None

    response = session.get(
        f"{WB_BASE_URL}/api/advert/v2/adverts",
        headers=headers,
        params=params,
        timeout=30,
    )
    if response.status_code == 404:
        legacy_url = f"{WB_BASE_URL}/adv/v1/promotion/adverts"
        response = session.get(legacy_url, headers=headers, timeout=30)
        if response.status_code == 405:
            response = session.post(legacy_url, headers=headers, json={}, timeout=30)

    if response.status_code in (401, 403):
        raise PermissionError("WB token is invalid or has no access.")
    if response.status_code == 429:
        raise RuntimeError("WB API rate limit reached while fetching campaigns.")
    if response.status_code >= 500:
        raise RuntimeError(
            f"WB adverts endpoint temporary error: {response.status_code}"
        )
    if response.status_code != 200:
        raise RuntimeError(
            f"WB adverts request failed: {response.status_code} {response.text[:300]}"
        )
    return response.json()


def validate_wb_token(
    session: requests.Session, token: str, logger: logging.Logger
) -> None:
    """Validate WB token with a test API call."""
    try:
        request_adverts(session, token)
    except PermissionError:
        logger.error("WB token validation failed: invalid token.")
        raise
    except requests.RequestException as exc:
        logger.error("WB token validation request error: %s", exc)
        raise
    logger.info("WB Token validated.")


def extract_adverts(payload: Any) -> list[dict[str, Any]]:
    """Extract adverts list from WB campaigns payload."""
    if isinstance(payload, dict):
        payload_adverts = payload.get("adverts", [])
        if isinstance(payload_adverts, list):
            return [item for item in payload_adverts if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def extract_campaign_id(record: dict[str, Any]) -> Optional[int]:
    """Extract campaign identifier from a record."""
    for key in ("campaign_id", "campaignId", "advertId", "advert_id", "id"):
        if key in record and record[key] is not None:
            try:
                return int(record[key])
            except (ValueError, TypeError):
                return None
    return None


def get_active_campaign_ids(payload: Any, logger: logging.Logger) -> list[int]:
    """Extract active campaign IDs from `/api/advert/v2/adverts` payload."""
    adverts = extract_adverts(payload)

    active_ids: list[int] = []
    for advert in adverts:
        campaign_id = extract_campaign_id(advert)
        if campaign_id is None:
            continue
        if advert.get("status") in (None, 9):
            active_ids.append(campaign_id)

    deduped_ids = list(dict.fromkeys(active_ids))
    logger.info("Found %d active campaigns.", len(deduped_ids))
    return deduped_ids


def get_campaign_ids_for_target_date(
    active_payload: Any,
    all_payload: Any,
    target_date: str,
    logger: logging.Logger,
) -> list[int]:
    """Build campaign IDs for target date: active + recently updated non-active."""
    active_ids = get_active_campaign_ids(active_payload, logger)
    selected: dict[int, int] = {campaign_id: campaign_id for campaign_id in active_ids}

    if not all_payload:
        logger.info(
            "Found %d campaigns for target date %s (only active list used).",
            len(selected),
            target_date,
        )
        return list(selected.values())

    all_adverts = extract_adverts(all_payload)
    lower_bound = (
        datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=STOPPED_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")

    extra_count = 0
    for advert in all_adverts:
        campaign_id = extract_campaign_id(advert)
        if campaign_id is None or campaign_id in selected:
            continue
        status_raw = advert.get("status")
        try:
            status = int(status_raw) if status_raw is not None else None
        except (TypeError, ValueError):
            status = None
        if status in (None, 9):
            continue

        timestamps = advert.get("timestamps")
        if not isinstance(timestamps, dict):
            continue

        started_day = normalize_iso_date(timestamps.get("started"))
        if started_day and started_day > target_date:
            continue

        updated_day = normalize_iso_date(timestamps.get("updated"))
        if not updated_day:
            continue
        if updated_day >= lower_bound:
            selected[campaign_id] = campaign_id
            extra_count += 1

    logger.info(
        "Found %d campaigns for target date %s (active: %d, added stopped in %d days: %d).",
        len(selected),
        target_date,
        len(active_ids),
        STOPPED_LOOKBACK_DAYS,
        extra_count,
    )
    return list(selected.values())


def chunked(items: list[int], size: int) -> Iterable[list[int]]:
    """Split list into chunks of fixed size."""
    for index in range(0, len(items), size):
        yield items[index : index + size]


def parse_retry_after_seconds(response: requests.Response) -> Optional[int]:
    """Parse Retry-After header (seconds)."""
    value = response.headers.get("Retry-After", "").strip()
    if not value:
        return None
    try:
        return max(1, int(float(value)))
    except (TypeError, ValueError):
        return None


def request_fullstats_batch(
    session: requests.Session,
    token: str,
    campaign_ids: list[int],
    target_date: str,
    logger: logging.Logger,
) -> Optional[Any]:
    """Request fullstats for one batch of campaign IDs with retry on HTTP 429."""
    url = f"{WB_BASE_URL}/adv/v3/fullstats"
    headers = build_wb_headers(token)
    params = {
        "ids": ",".join(str(campaign_id) for campaign_id in campaign_ids),
        "beginDate": target_date,
        "endDate": target_date,
    }
    rate_attempt = 0

    while True:
        try:
            response = session.get(url, headers=headers, params=params, timeout=60)
        except requests.RequestException as exc:
            logger.error("WB fullstats request error for batch %s: %s", campaign_ids, exc)
            return None

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError as exc:
                logger.error(
                    "Invalid JSON in WB fullstats response for batch %s: %s",
                    campaign_ids,
                    exc,
                )
                return None
        if response.status_code == 429:
            rate_attempt += 1
            if rate_attempt > MAX_429_RETRIES:
                logger.error(
                    "429 retries exceeded for batch %s (%d attempts).",
                    campaign_ids,
                    MAX_429_RETRIES,
                )
                return None
            retry_after_seconds = parse_retry_after_seconds(response)
            exponential_backoff = min(
                RATE_LIMIT_SLEEP_SECONDS * (2 ** (rate_attempt - 1)),
                MAX_RATE_LIMIT_SLEEP_SECONDS,
            )
            sleep_seconds = max(retry_after_seconds or 0, exponential_backoff)
            logger.warning(
                "Received 429 for batch %s (attempt %d/%d). Sleeping %s seconds...",
                campaign_ids,
                rate_attempt,
                MAX_429_RETRIES,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            continue
        if response.status_code >= 500:
            logger.error(
                "WB fullstats server error %s for batch %s. Skipping batch.",
                response.status_code,
                campaign_ids,
            )
            return None

        logger.error(
            "WB fullstats request failed (%s) for batch %s: %s",
            response.status_code,
            campaign_ids,
            response.text[:300],
        )
        return None


def fetch_fullstats(
    session: requests.Session,
    token: str,
    campaign_ids: list[int],
    target_date: str,
    logger: logging.Logger,
) -> list[Any]:
    """Fetch statistics for all campaign IDs in batches."""
    if not campaign_ids:
        return []

    all_payloads: list[Any] = []
    batches = list(chunked(campaign_ids, MAX_BATCH_SIZE))

    for batch_index, batch_ids in enumerate(batches, start=1):
        logger.info(
            "Fetching stats for batch %d (%d IDs)...",
            batch_index,
            len(batch_ids),
        )
        batch_payload = request_fullstats_batch(
            session=session,
            token=token,
            campaign_ids=batch_ids,
            target_date=target_date,
            logger=logger,
        )
        if batch_payload is not None:
            all_payloads.append(batch_payload)
            logger.info("Loaded data for campaigns: %s", batch_ids)

        if batch_index < len(batches):
            time.sleep(REQUEST_DELAY_SECONDS)

    return all_payloads


def as_float(value: Any) -> float:
    """Convert value to float safely."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: Any) -> int:
    """Convert value to integer safely."""
    return int(round(as_float(value)))


def round2(value: float) -> float:
    """Round numeric value to 2 decimal places."""
    return round(float(value), 2)


def safe_div(numerator: float, denominator: float) -> float:
    """Safe division with 0 fallback."""
    if denominator == 0:
        return 0.0
    return numerator / denominator


def normalize_iso_date(value: Any) -> str:
    """Normalize WB datetime/date values to YYYY-MM-DD."""
    if value is None:
        return ""
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return text


def map_conversion_type(app_type: Any) -> str:
    """Map WB appType to conversion type label."""
    try:
        app_type_int = int(app_type)
    except (TypeError, ValueError):
        return ""
    return CONVERSION_TYPE_MAP.get(app_type_int, f"Тип {app_type_int}")


def collect_campaign_items(payloads: list[Any]) -> list[dict[str, Any]]:
    """Collect campaign objects from all fullstats payload batches."""
    campaigns: list[dict[str, Any]] = []
    for payload in payloads:
        if isinstance(payload, list):
            campaigns.extend(item for item in payload if isinstance(item, dict))
            continue
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                campaigns.extend(item for item in data if isinstance(item, dict))
            elif isinstance(data, dict):
                campaigns.append(data)
    return campaigns


def build_booster_position_map(campaign: dict[str, Any], target_date: str) -> dict[int, float]:
    """Build map {nmId: avg_position} for selected date."""
    position_by_nm: dict[int, float] = {}
    booster_rows = campaign.get("boosterStats", [])
    if not isinstance(booster_rows, list):
        return position_by_nm

    for row in booster_rows:
        if not isinstance(row, dict):
            continue
        row_date = normalize_iso_date(row.get("date"))
        if row_date != target_date:
            continue
        nm_id = row.get("nm")
        if nm_id is None:
            continue
        try:
            nm_id_int = int(nm_id)
        except (TypeError, ValueError):
            continue
        position_by_nm[nm_id_int] = round2(as_float(row.get("avg_position")))
    return position_by_nm


def metric_values(stats: dict[str, Any]) -> dict[str, Any]:
    """Compute final metrics from a WB stats record."""
    spend = as_float(stats.get("sum"))
    orders_sum = as_float(stats.get("sum_price"))
    views = as_int(stats.get("views"))
    clicks = as_int(stats.get("clicks"))
    atbs = as_int(stats.get("atbs"))
    shks = as_int(stats.get("shks"))
    canceled = as_int(stats.get("canceled"))

    ctr = stats.get("ctr")
    if ctr is None:
        ctr = safe_div(clicks * 100.0, views)
    cr = stats.get("cr")
    if cr is None:
        cr = safe_div(shks * 100.0, clicks)
    cpc = stats.get("cpc")
    if cpc is None:
        cpc = safe_div(spend, clicks)

    return {
        "Затраты, RUB": round2(spend),
        "Заказов на сумму, RUB": round2(orders_sum),
        "Показы": views,
        "Клики": clicks,
        "Добавлений в корзину": atbs,
        "Заказанные товары, шт.": shks,
        "CTR(%)": round2(as_float(ctr)),
        "CR (%)": round2(as_float(cr)),
        "CPM": round2(safe_div(spend * 1000.0, views)),
        "CPC": round2(as_float(cpc)),
        "CPO": round2(safe_div(spend, shks)),
        "Отмены": canceled,
    }


def build_item_row(
    campaign_id: int,
    nm: dict[str, Any],
    conversion_type: str,
    avg_position: float,
    target_date: str,
) -> dict[str, Any]:
    """Build one товарная строка for Google Sheets."""
    nm_id = as_int(nm.get("nmId"))
    row = {
        "id РК": "",
        "Название": nm.get("name", ""),
        "Номенклатура": nm_id,
        "Средняя позиция": round2(avg_position),
        "Номер мультикарточки": "",
        "Тип конверсии": conversion_type,
        "report_date": target_date,
        "_campaign_id": campaign_id,
        "_row_type": "item",
        "_nm_id": nm_id,
    }
    row.update(metric_values(nm))
    return row


def build_total_row(
    campaign_id: int, total_stats: dict[str, Any], target_date: str
) -> dict[str, Any]:
    """Build one `Всего по кампании` row."""
    row = {
        "id РК": campaign_id,
        "Название": "Всего по кампании",
        "Номенклатура": "",
        "Средняя позиция": "",
        "Номер мультикарточки": "",
        "Тип конверсии": "",
        "report_date": target_date,
        "_campaign_id": campaign_id,
        "_row_type": "total",
        "_nm_id": 0,
    }
    row.update(metric_values(total_stats))
    return row


def build_dataframe(payloads: list[Any], target_date: str, logger: logging.Logger) -> pd.DataFrame:
    """Create table in manual-report structure for one target date."""
    campaign_items = collect_campaign_items(payloads)
    if not campaign_items:
        logger.warning("No data returned for target date %s.", target_date)
        return pd.DataFrame(columns=REPORT_COLUMNS)

    rows: list[dict[str, Any]] = []
    for campaign in campaign_items:
        campaign_id = extract_campaign_id(campaign)
        if campaign_id is None:
            continue

        booster_position_map = build_booster_position_map(campaign, target_date)
        days = campaign.get("days", [])
        if not isinstance(days, list):
            days = []

        has_target_day = False
        for day in days:
            if not isinstance(day, dict):
                continue

            day_date = normalize_iso_date(day.get("date"))
            if day_date and day_date != target_date:
                continue

            has_target_day = True
            apps = day.get("apps", [])
            if not isinstance(apps, list):
                apps = []

            for app in apps:
                if not isinstance(app, dict):
                    continue
                conversion_type = map_conversion_type(app.get("appType"))
                nms = app.get("nms", [])
                if not isinstance(nms, list):
                    nms = []

                for nm in nms:
                    if not isinstance(nm, dict):
                        continue
                    nm_id = as_int(nm.get("nmId"))
                    avg_position = booster_position_map.get(nm_id, 0.0)
                    rows.append(
                        build_item_row(
                            campaign_id=campaign_id,
                            nm=nm,
                            conversion_type=conversion_type,
                            avg_position=avg_position,
                            target_date=target_date,
                        )
                    )

            rows.append(build_total_row(campaign_id, day, target_date))

        if not has_target_day:
            rows.append(build_total_row(campaign_id, campaign, target_date))

    if not rows:
        logger.warning("No rows built for target date %s.", target_date)
        return pd.DataFrame(columns=REPORT_COLUMNS)

    df = pd.DataFrame(rows)

    before_drop = len(df)
    df = df.drop_duplicates(
        subset=["_campaign_id", "report_date", "_row_type", "_nm_id", "Тип конверсии"],
        keep="first",
    )
    removed = before_drop - len(df)
    if removed > 0:
        logger.info("Duplicates removed: %d", removed)

    numeric_columns = [
        "Затраты, RUB",
        "Заказов на сумму, RUB",
        "Показы",
        "Клики",
        "Добавлений в корзину",
        "Заказанные товары, шт.",
        "CTR(%)",
        "CR (%)",
        "CPM",
        "CPC",
        "CPO",
        "Отмены",
        "Средняя позиция",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = df[column].fillna(0)

    df = df[REPORT_COLUMNS]
    logger.info("Data loaded. Rows: %d.", len(df))
    return df


def ensure_sheet_exists(service: Any, spreadsheet_id: str, sheet_name: str) -> None:
    """Create sheet if it does not exist."""
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {
        sheet["properties"]["title"]
        for sheet in metadata.get("sheets", [])
        if "properties" in sheet and "title" in sheet["properties"]
    }
    if sheet_name in existing:
        return

    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def serialize_cell(value: Any) -> Any:
    """Convert non-scalar values to a printable representation."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def upload_to_google_sheets(df: pd.DataFrame, config: Config, logger: logging.Logger) -> None:
    """Upload DataFrame content to Google Sheets."""
    credentials = service_account.Credentials.from_service_account_file(
        config.google_credentials_file, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    ensure_sheet_exists(service, config.google_spreadsheet_id, config.google_sheet_name)

    clear_range = f"{config.google_sheet_name}!A1:Z10000"
    service.spreadsheets().values().clear(
        spreadsheetId=config.google_spreadsheet_id,
        range=clear_range,
        body={},
    ).execute()

    values = [df.columns.tolist()]
    values.extend(
        [
            [serialize_cell(cell) for cell in row]
            for row in df.itertuples(index=False, name=None)
        ]
    )

    service.spreadsheets().values().update(
        spreadsheetId=config.google_spreadsheet_id,
        range=f"{config.google_sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()
    logger.info(
        "Data successfully uploaded to Google Sheets (ID: %s).",
        config.google_spreadsheet_id,
    )


def main() -> int:
    """Main entry point."""
    load_dotenv()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger = setup_logging(log_level=log_level)
    logger.info("Start script.")

    try:
        config = load_config()
    except Exception:
        logger.exception("Configuration validation failed.")
        return 1

    session = requests.Session()

    try:
        validate_wb_token(session=session, token=config.wb_token, logger=logger)

        try:
            campaigns_payload_active = request_adverts(session, config.wb_token, statuses="9")
        except requests.RequestException:
            logger.exception("Campaign list request failed.")
            return 1

        campaigns_payload_all: Any = None
        try:
            campaigns_payload_all = request_adverts(session, config.wb_token, statuses=None)
        except Exception as exc:  # noqa: BLE001 - non-critical fallback
            logger.warning(
                "Failed to load all campaigns list, fallback to active only: %s",
                exc,
            )

        campaign_ids = get_campaign_ids_for_target_date(
            active_payload=campaigns_payload_active,
            all_payload=campaigns_payload_all,
            target_date=config.target_date,
            logger=logger,
        )

        stats_payloads = fetch_fullstats(
            session=session,
            token=config.wb_token,
            campaign_ids=campaign_ids,
            target_date=config.target_date,
            logger=logger,
        )

        dataframe = build_dataframe(stats_payloads, config.target_date, logger)
        if config.filter_zero_spend and not dataframe.empty and "Затраты, RUB" in dataframe.columns:
            before = len(dataframe)
            dataframe = dataframe[pd.to_numeric(dataframe["Затраты, RUB"], errors="coerce").fillna(0) > 0].copy()
            removed = before - len(dataframe)
            if removed > 0:
                logger.info("Filtered zero-spend rows: %d removed.", removed)
        upload_to_google_sheets(dataframe, config, logger)
        logger.info("Script finished successfully. Processed rows: %d", len(dataframe))
        return 0

    except (PermissionError, HttpError):
        logger.exception("Critical error during script execution.")
        return 1
    except Exception:
        logger.exception("Critical error during script execution.")
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
