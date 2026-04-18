"""WB Analytics API client for own-brand search positions."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Optional

import requests

DEFAULT_WB_ANALYTICS_POSITION_URL = (
    "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/orders"
)
WB_ANALYTICS_MAX_RETRIES = 4
WB_ANALYTICS_TIMEOUT_SECONDS = 30
WB_ANALYTICS_RETRY_BASE_SLEEP_SECONDS = 1.0
MAX_RAW_PAYLOAD_LENGTH = 5000


class WBAnalyticsClient:
    """Client for retrieving own-brand position by search query."""

    POSITION_KEYS = ("avgPosition", "avg_position", "position", "pos", "place", "rank")
    DATE_KEYS = ("dt", "date", "day")

    def __init__(
        self,
        token: str,
        logger: logging.Logger,
        position_url: Optional[str] = None,
    ):
        self.token = token.strip()
        self.logger = logger
        self.position_url = (position_url or os.getenv("WB_ANALYTICS_POSITION_URL", "")).strip()
        if not self.position_url:
            self.position_url = DEFAULT_WB_ANALYTICS_POSITION_URL
        self.session = requests.Session()

    @classmethod
    def from_env_optional(cls, logger: logging.Logger) -> Optional["WBAnalyticsClient"]:
        """Build optional client from env; return None if token is absent."""
        token = os.getenv("WB_ANALYTICS_TOKEN", "").strip()
        if not token:
            return None
        return cls(
            token=token,
            logger=logger,
            position_url=os.getenv("WB_ANALYTICS_POSITION_URL", "").strip() or None,
        )

    def close(self) -> None:
        """Close underlying HTTP session."""
        self.session.close()

    @property
    def headers(self) -> dict[str, str]:
        """Build headers for WB Analytics API."""
        return {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def fetch_position(
        self,
        nm_id: int,
        query: str,
        target_date: str,
    ) -> tuple[Optional[int], str, str, str]:
        """Fetch position for a single (nm_id, query) pair from WB Analytics."""
        normalized_query = self._normalize_query(query)
        if not normalized_query:
            return None, "error", "Empty query.", ""

        day = str(target_date).strip()
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            return None, "error", f"Invalid target_date: {day}", ""

        payload = {
            "nmIDs": [int(nm_id)],
            "searchTexts": [str(query).strip()],
            "period": {
                "begin": day,
                "end": day,
            },
            "orderBy": {
                "field": "avgPosition",
                "mode": "asc",
            },
        }

        attempt = 0
        while attempt < WB_ANALYTICS_MAX_RETRIES:
            attempt += 1
            try:
                response = self.session.post(
                    self.position_url,
                    headers=self.headers,
                    json=payload,
                    timeout=WB_ANALYTICS_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt >= WB_ANALYTICS_MAX_RETRIES:
                    return None, "error", f"Network error after {WB_ANALYTICS_MAX_RETRIES} attempts: {exc}", ""
                time.sleep(WB_ANALYTICS_RETRY_BASE_SLEEP_SECONDS * attempt)
                continue

            if response.status_code == 200:
                try:
                    body = response.json()
                except ValueError:
                    return None, "error", "Invalid JSON response.", response.text[:MAX_RAW_PAYLOAD_LENGTH]

                item = self._find_query_item(body, normalized_query)
                raw_payload = self._serialize_payload(body)
                if item is None:
                    return None, "not_found", "", raw_payload

                position = self._extract_position_from_item(item, target_date=day)
                if position is None:
                    return None, "not_found", "", raw_payload
                return position, "ok", "", raw_payload

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt >= WB_ANALYTICS_MAX_RETRIES:
                    message = f"HTTP {response.status_code} after {WB_ANALYTICS_MAX_RETRIES} attempts."
                    return None, "error", message, response.text[:MAX_RAW_PAYLOAD_LENGTH]
                time.sleep(WB_ANALYTICS_RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1)))
                continue

            if response.status_code in (401, 403):
                message = f"HTTP {response.status_code}: access denied."
                return None, "error", message, response.text[:MAX_RAW_PAYLOAD_LENGTH]

            message = f"Unexpected HTTP {response.status_code}: {response.text[:250]}"
            return None, "error", message, response.text[:MAX_RAW_PAYLOAD_LENGTH]

        return None, "error", "Retry loop ended unexpectedly.", ""

    def _find_query_item(self, payload: Any, normalized_query: str) -> Optional[dict[str, Any]]:
        items = self._extract_items(payload)
        if not items:
            return None

        for item in items:
            if not isinstance(item, dict):
                continue
            for key in ("text", "searchText", "query", "word", "norm_query"):
                if self._normalize_query(item.get(key)) == normalized_query:
                    return item

        # If API returns exactly one row, treat it as target query.
        if len(items) == 1 and isinstance(items[0], dict):
            return items[0]
        return None

    @staticmethod
    def _extract_items(payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        data = payload.get("data")
        if isinstance(data, dict):
            items = data.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        elif isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        items = payload.get("items")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
        return []

    def _extract_position_from_item(self, item: dict[str, Any], target_date: str) -> Optional[int]:
        for key in ("total", "stats", "days", "rows"):
            position = self._extract_position_from_series(item.get(key), target_date=target_date)
            if position is not None:
                return position

        for key in self.POSITION_KEYS:
            position = self._to_positive_int(item.get(key))
            if position is not None:
                return position
        return None

    def _extract_position_from_series(self, series: Any, target_date: str) -> Optional[int]:
        if isinstance(series, dict):
            position = self._extract_position_from_item(series, target_date=target_date)
            if position is not None:
                return position
            return None

        if not isinstance(series, list):
            return None

        matched: list[int] = []
        fallback: list[int] = []
        for row in series:
            if not isinstance(row, dict):
                continue
            row_position = None
            for key in self.POSITION_KEYS:
                row_position = self._to_positive_int(row.get(key))
                if row_position is not None:
                    break
            if row_position is None:
                continue

            if self._row_matches_date(row, target_date):
                matched.append(row_position)
            else:
                fallback.append(row_position)

        if matched:
            return matched[0]
        if fallback:
            return fallback[0]
        return None

    def _row_matches_date(self, row: dict[str, Any], target_date: str) -> bool:
        for key in self.DATE_KEYS:
            if key not in row:
                continue
            normalized = self._normalize_date(row.get(key))
            if normalized == target_date:
                return True
        return False

    @staticmethod
    def _normalize_query(value: Any) -> str:
        text = str(value or "").strip().lower().replace("\u0451", "\u0435")
        return " ".join(text.split())

    @staticmethod
    def _normalize_date(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text:
            return ""
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        if len(text) >= 10 and text[2] == "." and text[5] == ".":
            try:
                return datetime.strptime(text[:10], "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                return ""
        if len(text) >= 5 and text[2] == ".":
            # Date without year (e.g. 10.04) cannot be matched unambiguously here.
            return ""
        return ""

    @staticmethod
    def _to_positive_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            result = int(round(float(value)))
            return result if result > 0 else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _serialize_payload(payload: Any) -> str:
        try:
            text = json.dumps(payload, ensure_ascii=False)
        except TypeError:
            text = str(payload)
        if len(text) > MAX_RAW_PAYLOAD_LENGTH:
            return text[:MAX_RAW_PAYLOAD_LENGTH]
        return text

