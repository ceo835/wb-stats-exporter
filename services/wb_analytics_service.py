"""WB Analytics API client for own-brand search positions."""

from __future__ import annotations

from dataclasses import dataclass
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
DEFAULT_WB_ANALYTICS_SEARCH_TEXTS_URL = (
    "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
)
WB_ANALYTICS_MAX_RETRIES = 4
WB_ANALYTICS_TIMEOUT_SECONDS = 30
WB_ANALYTICS_RETRY_BASE_SLEEP_SECONDS = 1.0
MAX_RAW_PAYLOAD_LENGTH = 5000
DEFAULT_WB_SEARCH_TEXTS_LIMIT = 30
DEFAULT_WB_SEARCH_TEXTS_TOP_ORDER_BY = "orders"


@dataclass
class WBAnalyticsSearchResult:
    """Normalized WB Analytics result for one (nm_id, query)."""

    position: Optional[int]
    matched_query: str
    match_type: str
    traffic_volume: Optional[int]
    status: str
    error_msg: str
    raw_payload: str


class WBAnalyticsClient:
    """Client for retrieving own-brand position by search query."""

    POSITION_KEYS = ("avgPosition", "avg_position", "position", "pos", "place", "rank")
    DATE_KEYS = ("dt", "date", "day")

    def __init__(
        self,
        token: str,
        logger: logging.Logger,
        position_url: Optional[str] = None,
        search_texts_url: Optional[str] = None,
    ):
        self.token = token.strip()
        self.logger = logger
        self.position_url = (position_url or os.getenv("WB_ANALYTICS_POSITION_URL", "")).strip()
        if not self.position_url:
            self.position_url = DEFAULT_WB_ANALYTICS_POSITION_URL
        self.search_texts_url = (search_texts_url or os.getenv("WB_ANALYTICS_SEARCH_TEXTS_URL", "")).strip()
        if not self.search_texts_url:
            self.search_texts_url = DEFAULT_WB_ANALYTICS_SEARCH_TEXTS_URL
        self.session = requests.Session()
        self._search_texts_cache: dict[tuple[int, str], dict[str, Any]] = {}

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
            search_texts_url=os.getenv("WB_ANALYTICS_SEARCH_TEXTS_URL", "").strip() or None,
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
        result = self.fetch_search_result(nm_id=nm_id, query=query, target_date=target_date)
        legacy_status = {
            "found": "ok",
            "not_found": "not_found",
            "source_error": "error",
        }.get(result.status, "error")
        return result.position, legacy_status, result.error_msg, result.raw_payload

    def fetch_search_result(
        self,
        nm_id: int,
        query: str,
        target_date: str,
    ) -> WBAnalyticsSearchResult:
        """Fetch official WB position for a single (nm_id, query) pair."""
        normalized_query = self._normalize_query(query)
        if not normalized_query:
            return WBAnalyticsSearchResult(
                position=None,
                matched_query="",
                match_type="not_found",
                traffic_volume=None,
                status="source_error",
                error_msg="Empty query.",
                raw_payload="",
            )

        day = str(target_date).strip()
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            return WBAnalyticsSearchResult(
                position=None,
                matched_query="",
                match_type="not_found",
                traffic_volume=None,
                status="source_error",
                error_msg=f"Invalid target_date: {day}",
                raw_payload="",
            )

        search_texts_payload, search_texts_error = self._fetch_search_texts_payload(nm_id=int(nm_id), day=day)
        if search_texts_payload is None:
            return WBAnalyticsSearchResult(
                position=None,
                matched_query="",
                match_type="not_found",
                traffic_volume=None,
                status="source_error",
                error_msg=search_texts_error,
                raw_payload="",
            )

        matched_query, match_type, item = self._select_search_text_match(
            normalized_query=normalized_query,
            payload=search_texts_payload,
        )

        direct_query = str(query or "").strip()
        if not matched_query:
            matched_query = direct_query
        traffic_volume = self._extract_frequency_from_item(item) if item else None

        position_payload, position_error = self._fetch_orders_payload(
            nm_id=int(nm_id),
            query_text=matched_query,
            day=day,
        )
        combined_payload = {
            "search_texts": search_texts_payload,
            "orders": position_payload or {},
        }
        raw_payload = self._serialize_payload(combined_payload)

        if position_payload is None:
            return WBAnalyticsSearchResult(
                position=None,
                matched_query=matched_query,
                match_type=match_type,
                traffic_volume=traffic_volume,
                status="source_error",
                error_msg=position_error,
                raw_payload=raw_payload,
            )

        position_item = self._find_query_item(position_payload, self._normalize_query(matched_query))
        if position_item is None and direct_query and matched_query != direct_query:
            direct_payload, direct_error = self._fetch_orders_payload(
                nm_id=int(nm_id),
                query_text=direct_query,
                day=day,
            )
            if direct_payload is None:
                return WBAnalyticsSearchResult(
                    position=None,
                    matched_query=matched_query,
                    match_type=match_type,
                    traffic_volume=traffic_volume,
                    status="source_error",
                    error_msg=direct_error,
                    raw_payload=raw_payload,
                )
            position_payload = direct_payload
            position_item = self._find_query_item(position_payload, normalized_query)
            if position_item is not None:
                matched_query = direct_query
                match_type = "exact"
            raw_payload = self._serialize_payload(
                {"search_texts": search_texts_payload, "orders": position_payload}
            )

        if position_item is None:
            return WBAnalyticsSearchResult(
                position=None,
                matched_query=matched_query,
                match_type=match_type,
                traffic_volume=traffic_volume,
                status="not_found",
                error_msg="",
                raw_payload=raw_payload,
            )

        position = self._extract_position_from_item(position_item, target_date=day)
        if position is None:
            position = self._extract_position_from_item(item or {}, target_date=day)
        if position is None:
            return WBAnalyticsSearchResult(
                position=None,
                matched_query=matched_query,
                match_type=match_type,
                traffic_volume=traffic_volume,
                status="not_found",
                error_msg="",
                raw_payload=raw_payload,
            )
        return WBAnalyticsSearchResult(
            position=position,
            matched_query=matched_query,
            match_type=match_type,
            traffic_volume=traffic_volume,
            status="found",
            error_msg="",
            raw_payload=raw_payload,
        )

    def _select_search_text_match(
        self,
        normalized_query: str,
        payload: dict[str, Any],
    ) -> tuple[str, str, Optional[dict[str, Any]]]:
        items = self._extract_items(payload)
        if not items:
            return "", "not_found", None

        exact_fallback: Optional[tuple[str, dict[str, Any]]] = None
        for item in items:
            if not isinstance(item, dict):
                continue
            query_text = str(item.get("text") or item.get("searchText") or "").strip()
            if self._normalize_query(query_text) == normalized_query:
                if self._extract_position_from_item(item, target_date="") is not None:
                    return query_text, "exact", item
                if exact_fallback is None:
                    exact_fallback = (query_text, item)

        if exact_fallback is not None:
            return exact_fallback[0], "exact", exact_fallback[1]
        return "", "not_found", None

    def _fetch_search_texts_payload(self, nm_id: int, day: str) -> tuple[Optional[dict[str, Any]], str]:
        cache_key = (int(nm_id), day)
        cached_payload = self._search_texts_cache.get(cache_key)
        if cached_payload is not None:
            return cached_payload, ""

        try:
            limit = max(1, int(os.getenv("WB_ANALYTICS_SEARCH_TEXTS_LIMIT", str(DEFAULT_WB_SEARCH_TEXTS_LIMIT)).strip()))
        except ValueError:
            limit = DEFAULT_WB_SEARCH_TEXTS_LIMIT
        payload = {
            "currentPeriod": {
                "start": day,
                "end": day,
            },
            "nmIds": [int(nm_id)],
            "topOrderBy": os.getenv("WB_ANALYTICS_SEARCH_TEXTS_TOP_ORDER_BY", DEFAULT_WB_SEARCH_TEXTS_TOP_ORDER_BY).strip()
            or DEFAULT_WB_SEARCH_TEXTS_TOP_ORDER_BY,
            "includeSubstitutedSKUs": True,
            "includeSearchTexts": True,
            "orderBy": {
                "field": "avgPosition",
                "mode": "asc",
            },
            "limit": limit,
        }
        response_payload, error = self._post_json(self.search_texts_url, payload)
        if response_payload is not None:
            self._search_texts_cache[cache_key] = response_payload
        return response_payload, error

    def _fetch_orders_payload(self, nm_id: int, query_text: str, day: str) -> tuple[Optional[dict[str, Any]], str]:
        payload = {
            "period": {
                "start": day,
                "end": day,
            },
            "nmId": int(nm_id),
            "searchTexts": [str(query_text).strip()],
        }
        return self._post_json(self.position_url, payload)

    def _post_json(self, url: str, payload: dict[str, Any]) -> tuple[Optional[dict[str, Any]], str]:
        attempt = 0
        while attempt < WB_ANALYTICS_MAX_RETRIES:
            attempt += 1
            try:
                response = self.session.post(
                    url,
                    headers=self.headers,
                    json=payload,
                    timeout=WB_ANALYTICS_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt >= WB_ANALYTICS_MAX_RETRIES:
                    return None, f"Network error after {WB_ANALYTICS_MAX_RETRIES} attempts: {exc}"
                time.sleep(WB_ANALYTICS_RETRY_BASE_SLEEP_SECONDS * attempt)
                continue

            if response.status_code == 200:
                try:
                    body = response.json()
                except ValueError:
                    return None, "Invalid JSON response."
                if not isinstance(body, dict):
                    return None, "Unexpected payload type."
                return body, ""

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt >= WB_ANALYTICS_MAX_RETRIES:
                    return None, f"HTTP {response.status_code} after {WB_ANALYTICS_MAX_RETRIES} attempts."
                time.sleep(WB_ANALYTICS_RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1)))
                continue

            if response.status_code in (401, 403):
                return None, f"HTTP {response.status_code}: access denied."

            return None, f"Unexpected HTTP {response.status_code}: {response.text[:250]}"

        return None, "Retry loop ended unexpectedly."

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
        for key in ("dateItems", "total", "stats", "days", "rows"):
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

    def _extract_frequency_from_item(self, item: Optional[dict[str, Any]]) -> Optional[int]:
        if not isinstance(item, dict):
            return None
        frequency = item.get("frequency")
        if isinstance(frequency, dict):
            return self._to_positive_int(frequency.get("current"))
        return self._to_positive_int(frequency)

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
