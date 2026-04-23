"""MPSTATS API client for search position checks."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Optional

import requests


DEFAULT_MPSTATS_POSITION_URL = "https://mpstats.io/api/wb/get/item/{sku}/by_keywords"
MPSTATS_MAX_RETRIES = 4
MPSTATS_TIMEOUT_SECONDS = 30
MPSTATS_RETRY_BASE_SLEEP_SECONDS = 1.0
MAX_RAW_PAYLOAD_LENGTH = 5000


@dataclass
class MPStatsSearchResult:
    """Normalized MPSTATS result for one (nm_id, query)."""

    position: Optional[int]
    organic_position: Optional[int]
    boost_position: Optional[int]
    matched_query: str
    match_type: str
    traffic_volume: Optional[int]
    status: str
    error_msg: str
    raw_payload: str


class MPStatsClient:
    """Client for retrieving item position by search query."""

    POSITION_KEYS = (
        "position",
        "pos",
        "place",
        "search_position",
        "searchPosition",
        "rank",
    )
    LEGACY_URL_MARKER = "search-position"

    def __init__(
        self,
        token: str,
        logger: logging.Logger,
        position_url: Optional[str] = None,
    ):
        self.token = token.strip()
        self.logger = logger
        raw_url = (position_url or os.getenv("MPSTATS_POSITION_URL", "")).strip()
        self.position_url = self._resolve_position_url(raw_url)
        self.session = requests.Session()
        self._keywords_cache: dict[tuple[int, str, str], dict[str, Any]] = {}

    @classmethod
    def from_env(cls, logger: logging.Logger) -> "MPStatsClient":
        """Build client from environment variables."""
        token = os.getenv("MPSTATS_API_TOKEN", "").strip()
        if not token:
            raise RuntimeError("MPSTATS_API_TOKEN is empty.")
        return cls(
            token=token,
            logger=logger,
            position_url=os.getenv("MPSTATS_POSITION_URL", "").strip() or None,
        )

    def close(self) -> None:
        """Close underlying HTTP session."""
        self.session.close()

    @property
    def headers(self) -> dict[str, str]:
        """Build headers for MPSTATS API."""
        return {
            "X-Mpstats-TOKEN": self.token,
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def fetch_position(
        self,
        nm_id: int,
        query: str,
        target_date: Optional[str] = None,
    ) -> tuple[Optional[int], str, str, str]:
        """Legacy wrapper: fetch only position/status/error/raw_payload."""
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
        target_date: Optional[str] = None,
    ) -> MPStatsSearchResult:
        """Fetch structured search result with matching metadata."""
        normalized_query = self._normalize_query(query)
        if not normalized_query:
            return MPStatsSearchResult(
                position=None,
                organic_position=None,
                boost_position=None,
                matched_query="",
                match_type="not_found",
                traffic_volume=None,
                status="source_error",
                error_msg="Empty query.",
                raw_payload="",
            )

        day = (target_date or datetime.utcnow().strftime("%Y-%m-%d")).strip()
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            return MPStatsSearchResult(
                position=None,
                organic_position=None,
                boost_position=None,
                matched_query="",
                match_type="not_found",
                traffic_volume=None,
                status="source_error",
                error_msg=f"Invalid target_date: {day}",
                raw_payload="",
            )

        payload, fetch_error = self._fetch_keywords_payload(nm_id=int(nm_id), d1=day, d2=day)
        if payload is None:
            return MPStatsSearchResult(
                position=None,
                organic_position=None,
                boost_position=None,
                matched_query="",
                match_type="not_found",
                traffic_volume=None,
                status="source_error",
                error_msg=fetch_error,
                raw_payload="",
            )

        raw_payload = self._serialize_payload(payload)
        matched_query, match_type, row = self._select_best_match(query=query, payload=payload)
        if row is None:
            return MPStatsSearchResult(
                position=None,
                organic_position=None,
                boost_position=None,
                matched_query=matched_query,
                match_type=match_type,
                traffic_volume=None,
                status="not_found",
                error_msg="",
                raw_payload=raw_payload,
            )

        boost_position = self._extract_position_from_word_row(
            payload=payload,
            row=row,
            target_date=day,
            series_key="pos",
            avg_keys=("avgPos", "avg_pos", "avg_ad_pos"),
        )
        organic_position = self._extract_position_from_word_row(
            payload=payload,
            row=row,
            target_date=day,
            series_key="organic_pos",
            avg_keys=("avg_organic_pos",),
        )
        position = boost_position if boost_position is not None else organic_position
        traffic_volume = self._extract_traffic_volume(row)
        if position is None:
            return MPStatsSearchResult(
                position=None,
                organic_position=organic_position,
                boost_position=boost_position,
                matched_query=matched_query,
                match_type=match_type,
                traffic_volume=traffic_volume,
                status="not_found",
                error_msg="",
                raw_payload=raw_payload,
            )
        return MPStatsSearchResult(
            position=position,
            organic_position=organic_position,
            boost_position=boost_position,
            matched_query=matched_query,
            match_type=match_type,
            traffic_volume=traffic_volume,
            status="found",
            error_msg="",
            raw_payload=raw_payload,
        )

    def _select_best_match(
        self,
        query: str,
        payload: dict[str, Any],
    ) -> tuple[str, str, Optional[dict[str, Any]]]:
        words = payload.get("words")
        normalized_user_query = self._normalize_query(query)

        if isinstance(words, dict):
            return self._select_best_match_in_dict(normalized_user_query, words)
        if isinstance(words, list):
            return self._select_best_match_in_list(normalized_user_query, words)
        return "", "not_found", None

    def _select_best_match_in_dict(
        self,
        normalized_user_query: str,
        words_dict: dict[str, Any],
    ) -> tuple[str, str, Optional[dict[str, Any]]]:
        exact_fallback: Optional[tuple[str, dict[str, Any]]] = None
        norm_fallback: Optional[tuple[str, dict[str, Any]]] = None

        # 1) exact query match (prefer rows with a real position)
        for api_query, data in words_dict.items():
            if not isinstance(data, dict):
                continue
            if self._normalize_query(api_query) == normalized_user_query:
                query_text = str(api_query).strip()
                if self._extract_best_rank_for_match(data) < 999999:
                    return query_text, "exact", data
                if exact_fallback is None:
                    exact_fallback = (query_text, data)

        # 2) by norm_query (prefer rows with a real position)
        for api_query, data in words_dict.items():
            if not isinstance(data, dict):
                continue
            if self._normalize_query(data.get("norm_query")) == normalized_user_query:
                query_text = str(api_query).strip()
                if self._extract_best_rank_for_match(data) < 999999:
                    return query_text, "norm_query", data
                if norm_fallback is None:
                    norm_fallback = (query_text, data)

        if exact_fallback is not None:
            return exact_fallback[0], "exact", exact_fallback[1]
        if norm_fallback is not None:
            return norm_fallback[0], "norm_query", norm_fallback[1]
        return "", "not_found", None

    def _select_best_match_in_list(
        self,
        normalized_user_query: str,
        words_list: list[Any],
    ) -> tuple[str, str, Optional[dict[str, Any]]]:
        rows = [row for row in words_list if isinstance(row, dict)]
        if not rows:
            return "", "not_found", None

        exact_fallback: Optional[tuple[str, dict[str, Any]]] = None
        norm_fallback: Optional[tuple[str, dict[str, Any]]] = None

        # 1) exact (prefer rows with a real position)
        for row in rows:
            query_text = str(row.get("word") or row.get("query") or row.get("text") or "").strip()
            if self._normalize_query(query_text) == normalized_user_query:
                if self._extract_best_rank_for_match(row) < 999999:
                    return query_text, "exact", row
                if exact_fallback is None:
                    exact_fallback = (query_text, row)

        # 2) norm_query (prefer rows with a real position)
        for row in rows:
            query_text = str(row.get("word") or row.get("query") or row.get("text") or "").strip()
            if self._normalize_query(row.get("norm_query")) == normalized_user_query:
                if self._extract_best_rank_for_match(row) < 999999:
                    return query_text, "norm_query", row
                if norm_fallback is None:
                    norm_fallback = (query_text, row)

        if exact_fallback is not None:
            return exact_fallback[0], "exact", exact_fallback[1]
        if norm_fallback is not None:
            return norm_fallback[0], "norm_query", norm_fallback[1]
        return "", "not_found", None

    def _fetch_keywords_payload(self, nm_id: int, d1: str, d2: str) -> tuple[Optional[dict[str, Any]], str]:
        cache_key = (int(nm_id), d1, d2)
        cached_payload = self._keywords_cache.get(cache_key)
        if cached_payload is not None:
            return cached_payload, ""

        url = self._build_keywords_url(int(nm_id))
        params = {"d1": d1, "d2": d2}

        attempt = 0
        while attempt < MPSTATS_MAX_RETRIES:
            attempt += 1
            try:
                response = self.session.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=MPSTATS_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt >= MPSTATS_MAX_RETRIES:
                    return None, f"Network error after {MPSTATS_MAX_RETRIES} attempts: {exc}"
                time.sleep(MPSTATS_RETRY_BASE_SLEEP_SECONDS * attempt)
                continue

            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError:
                    return None, "Invalid JSON response."
                if not isinstance(payload, dict):
                    return None, "Unexpected payload type."
                self._keywords_cache[cache_key] = payload
                return payload, ""

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt >= MPSTATS_MAX_RETRIES:
                    return None, f"HTTP {response.status_code} after {MPSTATS_MAX_RETRIES} attempts."
                time.sleep(MPSTATS_RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1)))
                continue

            return None, f"Unexpected HTTP {response.status_code}: {response.text[:250]}"

        return None, "Retry loop ended unexpectedly."

    def _find_word_row(self, payload: dict[str, Any], normalized_query: str) -> Optional[dict[str, Any]]:
        """Legacy matcher kept for backward compatibility paths."""
        words = payload.get("words")
        if isinstance(words, dict):
            for key, row in words.items():
                if not isinstance(row, dict):
                    continue
                if self._normalize_query(key) == normalized_query:
                    return row
                if self._normalize_query(row.get("norm_query")) == normalized_query:
                    return row
            return None

        if isinstance(words, list):
            for row in words:
                if not isinstance(row, dict):
                    continue
                if self._normalize_query(row.get("word")) == normalized_query:
                    return row
                if self._normalize_query(row.get("query")) == normalized_query:
                    return row
                if self._normalize_query(row.get("norm_query")) == normalized_query:
                    return row
        return None

    def _extract_best_rank_for_match(self, row: dict[str, Any]) -> int:
        positions = row.get("pos")
        if isinstance(positions, list):
            numeric_positions = [self._to_int_allow_zero(value) for value in positions]
            numeric_positions = [value for value in numeric_positions if value is not None and value > 0]
            if numeric_positions:
                return min(numeric_positions)
        avg_position = self._to_int_or_none(row.get("avgPos"))
        if avg_position is not None:
            return avg_position
        avg_position = self._to_int_or_none(row.get("avg_pos"))
        if avg_position is not None:
            return avg_position
        for key in self.POSITION_KEYS:
            direct = self._to_int_or_none(row.get(key))
            if direct is not None:
                return direct
        return 999999

    def _extract_traffic_volume(self, row: dict[str, Any]) -> Optional[int]:
        for key in ("traffic_volume", "wb_count"):
            if key not in row:
                continue
            value = row.get(key)
            if isinstance(value, list):
                for item in value:
                    parsed = self._to_int_allow_zero(item)
                    if parsed is not None:
                        return parsed
                continue
            parsed = self._to_int_allow_zero(value)
            if parsed is not None:
                return parsed
        return None

    def _extract_position_from_word_row(
        self,
        payload: dict[str, Any],
        row: dict[str, Any],
        target_date: str,
        series_key: str = "pos",
        avg_keys: tuple[str, ...] = ("avgPos", "avg_pos"),
    ) -> Optional[int]:
        positions = row.get(series_key)
        if isinstance(positions, list) and positions:
            day_index = self._find_day_index(payload.get("days"), target_date)
            if day_index is not None and day_index < len(positions):
                day_position = self._to_int_or_none(positions[day_index])
                if day_position is not None:
                    return day_position

            for value in reversed(positions):
                fallback_position = self._to_int_or_none(value)
                if fallback_position is not None:
                    return fallback_position

        for key in avg_keys:
            avg_position = self._to_int_or_none(row.get(key))
            if avg_position is not None:
                return avg_position

        if series_key == "pos":
            for key in self.POSITION_KEYS:
                direct_position = self._to_int_or_none(row.get(key))
                if direct_position is not None:
                    return direct_position

        return None

    @staticmethod
    def _find_day_index(days: Any, target_date: str) -> Optional[int]:
        if not isinstance(days, list):
            return None

        variants = {
            target_date,
            datetime.strptime(target_date, "%Y-%m-%d").strftime("%d.%m"),
            datetime.strptime(target_date, "%Y-%m-%d").strftime("%d.%m.%Y"),
        }
        for index, value in enumerate(days):
            text = str(value).strip()
            if text in variants:
                return index
        return None

    @staticmethod
    def _normalize_query(value: Any) -> str:
        # Normalize Cyrillic yo to e to reduce query-key mismatches.
        text = str(value or "").strip().lower().replace("\u0451", "\u0435")
        return " ".join(text.split())

    @classmethod
    def _resolve_position_url(cls, raw_url: str) -> str:
        if not raw_url:
            return DEFAULT_MPSTATS_POSITION_URL
        if cls.LEGACY_URL_MARKER in raw_url:
            return DEFAULT_MPSTATS_POSITION_URL
        if "{sku}" in raw_url:
            return raw_url

        url = raw_url.rstrip("/")
        if re.search(r"/item/\d+/by_keywords$", url):
            return re.sub(r"/item/\d+/by_keywords$", "/item/{sku}/by_keywords", url)
        if url.endswith("/item/by_keywords"):
            return url.replace("/item/by_keywords", "/item/{sku}/by_keywords")
        return url

    def _build_keywords_url(self, sku: int) -> str:
        if "{sku}" in self.position_url:
            return self.position_url.format(sku=sku)
        if self.position_url.endswith("/by_keywords"):
            return self.position_url
        return f"{self.position_url.rstrip('/')}/{sku}/by_keywords"

    @staticmethod
    def _to_int_or_none(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            result = int(float(value))
            return result if result > 0 else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int_allow_zero(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(float(value))
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
