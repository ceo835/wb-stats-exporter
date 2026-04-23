"""WB Content API helper for resolving product names by nm_id."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

import requests

DEFAULT_WB_CONTENT_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
DEFAULT_CACHE_FILE = "nm_id_cache.json"
MAX_RETRIES = 3
RETRY_BASE_SLEEP_SECONDS = 1.0
TIMEOUT_SECONDS = 30


class WBContentNameResolver:
    """Resolve product names via cache and optional WB Content API."""

    def __init__(
        self,
        logger: logging.Logger,
        token: str = "",
        content_url: str = "",
        cache_path: str = "",
    ):
        self.logger = logger
        self.token = str(token or "").strip()
        self.content_url = str(content_url or "").strip() or DEFAULT_WB_CONTENT_URL
        self.cache_path = Path(str(cache_path or "").strip() or DEFAULT_CACHE_FILE)
        self._session = requests.Session()
        self._cache = self._load_cache()

    @classmethod
    def from_env(cls, logger: logging.Logger) -> "WBContentNameResolver":
        """Build resolver from environment."""
        token = (
            os.getenv("WB_CONTENT_TOKEN", "").strip()
            or os.getenv("WB_API_TOKEN", "").strip()
            or os.getenv("WB_TOKEN", "").strip()
        )
        return cls(
            logger=logger,
            token=token,
            content_url=os.getenv("WB_CONTENT_URL", "").strip() or DEFAULT_WB_CONTENT_URL,
            cache_path=os.getenv("POSITIONS_NM_CACHE_FILE", "").strip() or DEFAULT_CACHE_FILE,
        )

    def close(self) -> None:
        """Close HTTP resources and persist cache."""
        self._session.close()
        self._save_cache()

    def resolve_name(self, nm_id: int, configured_name: str = "") -> str:
        """Resolve product name with priority: settings -> cache -> WB Content API."""
        given = str(configured_name or "").strip()
        if given:
            self._cache[str(int(nm_id))] = given
            return given

        cache_key = str(int(nm_id))
        cached = str(self._cache.get(cache_key, "")).strip()
        if cached:
            return cached

        fetched = self._fetch_name_from_wb(int(nm_id))
        if fetched:
            self._cache[cache_key] = fetched
            return fetched
        return ""

    def _fetch_name_from_wb(self, nm_id: int) -> str:
        if not self.token:
            return ""

        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Endpoint supports rich filters; for robustness we search by text and verify nmID.
        payload = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {
                    "withPhoto": -1,
                    "textSearch": str(nm_id),
                },
            }
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._session.post(
                    self.content_url,
                    headers=headers,
                    json=payload,
                    timeout=TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    self.logger.warning("WB Content network error for nm_id=%s: %s", nm_id, exc)
                    return ""
                time.sleep(RETRY_BASE_SLEEP_SECONDS * attempt)
                continue

            if response.status_code == 200:
                try:
                    body = response.json()
                except ValueError:
                    return ""
                return self._extract_name(body, nm_id)

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt == MAX_RETRIES:
                    self.logger.warning("WB Content HTTP %s for nm_id=%s", response.status_code, nm_id)
                    return ""
                time.sleep(RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1)))
                continue

            # 401/403 and other statuses are non-retryable for this scope.
            return ""

        return ""

    @classmethod
    def _extract_name(cls, body: Any, nm_id: int) -> str:
        cards = cls._extract_cards(body)
        if not cards:
            return ""

        # First pass: strict nmID match.
        for card in cards:
            if cls._card_nm_id(card) == nm_id:
                name = cls._card_name(card)
                if name:
                    return name

        # Fallback: best-effort first non-empty title.
        for card in cards:
            name = cls._card_name(card)
            if name:
                return name
        return ""

    @staticmethod
    def _extract_cards(body: Any) -> list[dict[str, Any]]:
        if not isinstance(body, dict):
            return []

        if isinstance(body.get("cards"), list):
            return [card for card in body["cards"] if isinstance(card, dict)]

        data = body.get("data")
        if isinstance(data, dict) and isinstance(data.get("cards"), list):
            return [card for card in data["cards"] if isinstance(card, dict)]

        if isinstance(data, list):
            return [card for card in data if isinstance(card, dict)]
        return []

    @staticmethod
    def _card_nm_id(card: dict[str, Any]) -> Optional[int]:
        for key in ("nmID", "nmId", "nm_id"):
            try:
                if key in card and card.get(key) is not None:
                    return int(card.get(key))
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _card_name(card: dict[str, Any]) -> str:
        for key in ("title", "nmName", "imtName", "subjectName", "name"):
            value = str(card.get(key, "")).strip()
            if value:
                return value
        return ""

    def _load_cache(self) -> dict[str, str]:
        if not self.cache_path.is_file():
            return {}
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        normalized: dict[str, str] = {}
        for key, value in data.items():
            try:
                nm_id = str(int(key))
            except (TypeError, ValueError):
                continue
            text = str(value or "").strip()
            if text:
                normalized[nm_id] = text
        return normalized

    def _save_cache(self) -> None:
        if not self._cache:
            return
        try:
            self.cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            self.logger.warning("Could not persist nm_id cache: %s", self.cache_path)
