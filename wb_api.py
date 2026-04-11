"""Wildberries API client with retry, batching and in-session caching."""

from __future__ import annotations

import copy
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

import requests

WB_BASE_URL = "https://advert-api.wildberries.ru"
MAX_BATCH_SIZE = 50
BATCH_SLEEP_SECONDS = 20
RATE_LIMIT_SLEEP_SECONDS = 60
MAX_429_RETRIES = 5
MAX_NETWORK_RETRIES = 3
NETWORK_RETRY_BASE_SLEEP_SECONDS = 5

CONVERSION_TYPE_MAP = {
    1: "Прямая",
    32: "Ассоциированная",
    64: "Мультикарточка",
}


@dataclass
class Campaign:
    """Campaign metadata."""

    campaign_id: int
    campaign_name: str
    status: Optional[int] = None


class WBApiClient:
    """WB API client for campaigns and fullstats."""

    def __init__(self, token: str, logger: logging.Logger):
        self.token = token
        self.logger = logger
        self.session = requests.Session()
        self._stats_cache: dict[tuple[str, str, tuple[int, ...]], list[dict[str, Any]]] = {}

    def close(self) -> None:
        """Close underlying HTTP session."""
        self.session.close()

    @property
    def headers(self) -> dict[str, str]:
        """HTTP headers for WB API requests."""
        return {"Authorization": self.token, "Content-Type": "application/json"}

    def validate_token(self) -> None:
        """Validate token by fetching active campaigns."""
        campaigns = self.get_active_campaigns()
        if not campaigns:
            self.logger.warning("WB token validated, but no active campaigns found.")
            return
        self.logger.info("WB token validated.")

    def get_active_campaigns(self) -> list[Campaign]:
        """Load active campaigns with fallback to legacy endpoint."""
        response = self.session.get(
            f"{WB_BASE_URL}/api/advert/v2/adverts",
            headers=self.headers,
            params={"statuses": "9"},
            timeout=30,
        )

        if response.status_code == 404:
            response = self._legacy_campaigns_request()

        self._raise_for_auth(response, "campaign list")

        if response.status_code == 429:
            raise RuntimeError("WB API rate limit reached while loading campaigns.")
        if response.status_code >= 500:
            raise RuntimeError(f"WB campaigns endpoint temporary error: {response.status_code}")
        if response.status_code != 200:
            raise RuntimeError(
                f"WB campaigns request failed: {response.status_code} {response.text[:250]}"
            )

        payload = response.json()
        campaigns = self._parse_campaigns(payload)
        self.logger.info("Найдено активных кампаний: %d", len(campaigns))
        return campaigns

    def fetch_stats_rows(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Fetch and flatten WB stats for selected date range."""
        self._validate_date_string(start_date)
        self._validate_date_string(end_date)

        campaigns = self.get_active_campaigns()
        if not campaigns:
            return []

        campaign_ids = [campaign.campaign_id for campaign in campaigns]
        campaign_map = {campaign.campaign_id: campaign.campaign_name for campaign in campaigns}

        payloads = self.fetch_fullstats_batches(campaign_ids, start_date, end_date)
        rows = self._flatten_fullstats(payloads, campaign_map, start_date, end_date)
        return rows

    def fetch_fullstats_batches(
        self, campaign_ids: list[int], start_date: str, end_date: str
    ) -> list[dict[str, Any]]:
        """Load `/adv/v3/fullstats` with batching and retry."""
        cache_key = (start_date, end_date, tuple(sorted(campaign_ids)))
        if cache_key in self._stats_cache:
            self.logger.info("Использован кэш WB API для периода %s - %s.", start_date, end_date)
            return copy.deepcopy(self._stats_cache[cache_key])

        all_items: list[dict[str, Any]] = []
        batches = list(self._chunked(campaign_ids, MAX_BATCH_SIZE))

        for index, batch_ids in enumerate(batches, start=1):
            self.logger.info(
                "Запрос статистики: батч %d/%d (%d кампаний).",
                index,
                len(batches),
                len(batch_ids),
            )
            batch_items = self._fetch_fullstats_batch(batch_ids, start_date, end_date)
            if batch_items:
                all_items.extend(batch_items)
                self.logger.info("Загружены данные для кампаний: %s", batch_ids)

            if index < len(batches):
                time.sleep(BATCH_SLEEP_SECONDS)

        self._stats_cache[cache_key] = copy.deepcopy(all_items)
        return all_items

    def _fetch_fullstats_batch(
        self, batch_ids: list[int], start_date: str, end_date: str
    ) -> list[dict[str, Any]]:
        params = {
            "ids": ",".join(str(value) for value in batch_ids),
            "beginDate": start_date,
            "endDate": end_date,
        }
        network_attempt = 0
        rate_attempt = 0
        while True:
            try:
                response = self.session.get(
                    f"{WB_BASE_URL}/adv/v3/fullstats",
                    headers=self.headers,
                    params=params,
                    timeout=60,
                )
            except requests.RequestException as exc:
                network_attempt += 1
                if network_attempt > MAX_NETWORK_RETRIES:
                    self.logger.error(
                        "WB fullstats request error for %s after %d retries: %s",
                        batch_ids,
                        MAX_NETWORK_RETRIES,
                        exc,
                    )
                    return []
                sleep_seconds = NETWORK_RETRY_BASE_SLEEP_SECONDS * network_attempt
                self.logger.warning(
                    "Network error for batch %s (attempt %d/%d), retry in %d sec: %s",
                    batch_ids,
                    network_attempt,
                    MAX_NETWORK_RETRIES,
                    sleep_seconds,
                    exc,
                )
                time.sleep(sleep_seconds)
                continue

            self._raise_for_auth(response, "fullstats")

            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError as exc:
                    self.logger.error("Некорректный JSON для батча %s: %s", batch_ids, exc)
                    return []
                if isinstance(payload, list):
                    return [item for item in payload if isinstance(item, dict)]
                return []

            if response.status_code == 429:
                rate_attempt += 1
                if rate_attempt > MAX_429_RETRIES:
                    self.logger.error(
                        "Rate limit retries exceeded for batch %s (%d attempts).",
                        batch_ids,
                        MAX_429_RETRIES,
                    )
                    return []
                self.logger.warning(
                    "429 для батча %s (попытка %d/%d), ожидание %d сек.",
                    batch_ids,
                    rate_attempt,
                    MAX_429_RETRIES,
                    RATE_LIMIT_SLEEP_SECONDS,
                )
                time.sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            if response.status_code >= 500:
                self.logger.error(
                    "WB вернул %d для батча %s. Батч пропущен.",
                    response.status_code,
                    batch_ids,
                )
                return []

            self.logger.error(
                "Ошибка WB fullstats %d для батча %s: %s",
                response.status_code,
                batch_ids,
                response.text[:250],
            )
            return []

    def _flatten_fullstats(
        self,
        payloads: list[dict[str, Any]],
        campaign_map: dict[int, str],
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for campaign in payloads:
            campaign_id = self._to_int(campaign.get("advertId"))
            if campaign_id is None:
                continue

            campaign_name = campaign_map.get(campaign_id, "")
            booster_positions = self._build_booster_position_map(campaign, start_date, end_date)
            days = campaign.get("days", [])
            if not isinstance(days, list):
                continue

            for day in days:
                if not isinstance(day, dict):
                    continue
                day_date = self._normalize_date(day.get("date"))
                if not self._in_range(day_date, start_date, end_date):
                    continue

                base = {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "date": day_date,
                    "currency": campaign.get("currency", "RUB"),
                }

                rows.append(
                    {
                        **base,
                        **self._extract_metrics(day),
                        "row_type": "campaign_total",
                        "app_type": None,
                        "conversion_type": "",
                        "nm_id": None,
                        "nm_name": "",
                        "avg_position": 0.0,
                    }
                )

                apps = day.get("apps", [])
                if not isinstance(apps, list):
                    continue

                for app in apps:
                    if not isinstance(app, dict):
                        continue
                    app_type = self._to_int(app.get("appType"))
                    conversion_type = self._conversion_type_name(app_type)

                    nms = app.get("nms", [])
                    if not isinstance(nms, list):
                        continue

                    for nm in nms:
                        if not isinstance(nm, dict):
                            continue
                        nm_id = self._to_int(nm.get("nmId"))
                        rows.append(
                            {
                                **base,
                                **self._extract_metrics(nm),
                                "row_type": "item",
                                "app_type": app_type,
                                "conversion_type": conversion_type,
                                "nm_id": nm_id,
                                "nm_name": str(nm.get("name", "")),
                                "avg_position": booster_positions.get((day_date, nm_id), 0.0),
                            }
                        )

        return rows

    def _legacy_campaigns_request(self) -> requests.Response:
        legacy_url = f"{WB_BASE_URL}/adv/v1/promotion/adverts"
        response = self.session.get(legacy_url, headers=self.headers, timeout=30)
        if response.status_code == 405:
            response = self.session.post(legacy_url, headers=self.headers, json={}, timeout=30)
        return response

    def _parse_campaigns(self, payload: Any) -> list[Campaign]:
        campaigns: list[Campaign] = []

        if isinstance(payload, dict) and isinstance(payload.get("adverts"), list):
            for item in payload["adverts"]:
                if not isinstance(item, dict):
                    continue
                campaign_id = self._to_int(item.get("id"))
                if campaign_id is None:
                    continue
                status = self._to_int(item.get("status"))
                if status not in (None, 9):
                    continue
                name = str(item.get("settings", {}).get("name", "")).strip()
                campaigns.append(Campaign(campaign_id=campaign_id, campaign_name=name, status=status))
            return campaigns

        for record in self._walk_dicts(payload):
            campaign_id = self._to_int(
                record.get("id")
                or record.get("campaignId")
                or record.get("advertId")
                or record.get("campaign_id")
            )
            if campaign_id is None:
                continue
            status = record.get("status") or record.get("state")
            if not self._is_active_status(status):
                continue

            name = str(
                record.get("name")
                or record.get("campaignName")
                or record.get("advertName")
                or record.get("title")
                or ""
            ).strip()
            campaigns.append(Campaign(campaign_id=campaign_id, campaign_name=name, status=self._to_int(status)))

        unique: dict[int, Campaign] = {}
        for campaign in campaigns:
            unique[campaign.campaign_id] = campaign
        return list(unique.values())

    def _build_booster_position_map(
        self, campaign: dict[str, Any], start_date: str, end_date: str
    ) -> dict[tuple[str, Optional[int]], float]:
        booster_rows = campaign.get("boosterStats", [])
        if not isinstance(booster_rows, list):
            return {}

        positions: dict[tuple[str, Optional[int]], float] = {}
        for row in booster_rows:
            if not isinstance(row, dict):
                continue
            row_date = self._normalize_date(row.get("date"))
            if not self._in_range(row_date, start_date, end_date):
                continue
            nm_id = self._to_int(row.get("nm"))
            avg_position = self._to_float(row.get("avg_position"))
            positions[(row_date, nm_id)] = round(avg_position, 2)
        return positions

    @staticmethod
    def _extract_metrics(node: dict[str, Any]) -> dict[str, Any]:
        def to_float(value: Any) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        def to_int(value: Any) -> int:
            return int(round(to_float(value)))

        return {
            "spend": round(to_float(node.get("sum")), 2),
            "revenue": round(to_float(node.get("sum_price")), 2),
            "views": to_int(node.get("views")),
            "clicks": to_int(node.get("clicks")),
            "atbs": to_int(node.get("atbs")),
            "orders": to_int(node.get("orders")),
            "ordered_items": to_int(node.get("shks")),
            "canceled": to_int(node.get("canceled")),
        }

    @staticmethod
    def _is_active_status(status: Any) -> bool:
        if status is None:
            return True
        if isinstance(status, (int, float)):
            return int(status) == 9
        if isinstance(status, str):
            status_lower = status.lower()
            if any(marker in status_lower for marker in ("active", "актив", "running", "enabled")):
                return True
        return False

    @staticmethod
    def _conversion_type_name(app_type: Optional[int]) -> str:
        if app_type is None:
            return ""
        return CONVERSION_TYPE_MAP.get(app_type, f"Тип {app_type}")

    @staticmethod
    def _walk_dicts(value: Any) -> Iterable[dict[str, Any]]:
        if isinstance(value, dict):
            yield value
            for item in value.values():
                yield from WBApiClient._walk_dicts(item)
        elif isinstance(value, list):
            for item in value:
                yield from WBApiClient._walk_dicts(item)

    @staticmethod
    def _chunked(values: list[int], size: int) -> Iterable[list[int]]:
        for index in range(0, len(values), size):
            yield values[index : index + size]

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _normalize_date(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        return text

    @staticmethod
    def _validate_date_string(value: str) -> None:
        datetime.strptime(value, "%Y-%m-%d")

    @staticmethod
    def _in_range(day: str, start_date: str, end_date: str) -> bool:
        if not day:
            return False
        return start_date <= day <= end_date

    @staticmethod
    def _raise_for_auth(response: requests.Response, scope: str) -> None:
        if response.status_code in (401, 403):
            raise PermissionError(f"WB token invalid or access denied for {scope}.")
