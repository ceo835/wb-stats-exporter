"""Collector orchestration for daily search positions."""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from .mpstats_service import MPStatsClient, MPStatsSearchResult
from .positions_gsheets_service import PositionsSheetsService
from .positions_models import PositionRecord
from .wb_analytics_service import WBAnalyticsClient, WBAnalyticsSearchResult
from .wb_content_service import WBContentNameResolver

DEFAULT_TIMEZONE = "Europe/Moscow"
DEFAULT_REQUEST_PAUSE_SECONDS = 0.7
DEFAULT_WB_FALLBACK_ON_NOT_FOUND = False


class PositionsCollector:
    """Coordinates trigger-based collection and Sheets upsert."""

    def __init__(
        self,
        sheets_service: PositionsSheetsService,
        mpstats_client: MPStatsClient,
        wb_analytics_client: Optional[WBAnalyticsClient],
        wb_content_resolver: WBContentNameResolver,
        logger: logging.Logger,
    ):
        self.sheets_service = sheets_service
        self.mpstats_client = mpstats_client
        self.wb_analytics_client = wb_analytics_client
        self.wb_content_resolver = wb_content_resolver
        self.logger = logger
        self.timezone = ZoneInfo(os.getenv("POSITIONS_TIMEZONE", DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE)
        pause = os.getenv("POSITIONS_REQUEST_PAUSE_SECONDS", str(DEFAULT_REQUEST_PAUSE_SECONDS)).strip()
        try:
            self.request_pause_seconds = max(0.0, float(pause))
        except ValueError:
            self.request_pause_seconds = DEFAULT_REQUEST_PAUSE_SECONDS

        self.wb_fallback_on_not_found = self._to_bool(
            os.getenv("WB_FALLBACK_ON_NOT_FOUND", str(int(DEFAULT_WB_FALLBACK_ON_NOT_FOUND)))
        )

    def run(
        self,
        force: bool = False,
        max_pairs: int = 0,
        date_from: str = "",
        date_to: str = "",
    ) -> int:
        """Run positions collection if trigger is pending or force=True."""
        self.sheets_service.ensure_base_sheets()
        state = self.sheets_service.get_collector_state()
        now = self._now()
        now_iso = self._iso_now(now)

        if state.running and not force:
            self.logger.info("Positions collector is already running; skip.")
            return 0

        if not force and not state.trigger_pending:
            self.logger.info("No pending trigger for positions collector; skip.")
            return 0

        self.sheets_service.mark_running(run_time=now_iso)
        self.logger.info("Positions collector started. force=%s", force)

        try:
            settings_csv = os.getenv("POSITIONS_SETTINGS_CSV", "").strip()
            pairs = self.sheets_service.load_pairs(csv_path=settings_csv)
            if max_pairs > 0:
                pairs = pairs[:max_pairs]
            if not pairs:
                self.logger.warning("No pairs found in settings.")
                self.sheets_service.mark_success(run_time=now_iso, rows=0)
                return 0

            collection_dates = self._resolve_collection_dates(
                today=now.date(),
                date_from=date_from,
                date_to=date_to,
            )
            self.logger.info(
                "Collection dates resolved: %s",
                ", ".join(collection_dates[:5]) + (" ..." if len(collection_dates) > 5 else ""),
            )

            records: list[PositionRecord] = []
            product_name_map: dict[int, str] = {}
            for pair in pairs:
                if pair.nm_id not in product_name_map:
                    product_name_map[pair.nm_id] = self.wb_content_resolver.resolve_name(
                        pair.nm_id,
                        configured_name=pair.product_name,
                    )

            total = len(pairs) * len(collection_dates)
            current_step = 0
            for collection_date in collection_dates:
                self.logger.info("Collecting positions for date %s.", collection_date)
                for pair in pairs:
                    current_step += 1
                    result = self._collect_pair(
                        nm_id=pair.nm_id,
                        user_query=pair.user_query,
                        is_own_brand=pair.is_own_brand,
                        collection_date=collection_date,
                    )
                    collected_at = self._iso_now(self._now())
                    records.append(
                        PositionRecord(
                            date=collection_date,
                            collected_at=collected_at,
                            nm_id=pair.nm_id,
                            product_name=product_name_map.get(pair.nm_id, pair.product_name),
                            user_query=pair.user_query,
                            matched_query=result["matched_query"],
                            match_type=result["match_type"],
                            position=result["position"],
                            organic_position=result["organic_position"],
                            boost_position=result["boost_position"],
                            traffic_volume=result["traffic_volume"],
                            status=result["status"],
                            data_source=result["data_source"],
                            error_msg=result["error_msg"],
                        )
                    )
                    if current_step < total and self.request_pause_seconds > 0:
                        time.sleep(self.request_pause_seconds)

            written_rows = self.sheets_service.upsert_positions(records)
            matrix_enabled = self._to_bool(os.getenv("POSITIONS_MATRIX_ENABLED", "0"))
            earliest_collection_date = collection_dates[0] if collection_dates else now.date().isoformat()
            if matrix_enabled:
                start_from_month = self._to_bool(os.getenv("POSITIONS_MATRIX_FROM_MONTH_START", "1"))
                matrix_start_date = (
                    date.fromisoformat(earliest_collection_date).replace(day=1).isoformat() if start_from_month else ""
                )
                try:
                    matrix_count = self.sheets_service.refresh_query_matrix_sheets(start_date=matrix_start_date)
                    self.logger.info(
                        "Positions matrix sheets refreshed: %d (start_date=%s)",
                        matrix_count,
                        matrix_start_date or "all",
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("Matrix sheets refresh skipped due to error: %s", exc)

            category_matrix_enabled = self._to_bool(os.getenv("POSITIONS_CATEGORY_MATRIX_ENABLED", "1"))
            if category_matrix_enabled:
                start_from_month = self._to_bool(os.getenv("POSITIONS_MATRIX_FROM_MONTH_START", "1"))
                category_start_date = (
                    date.fromisoformat(earliest_collection_date).replace(day=1).isoformat()
                    if start_from_month
                    else ""
                )
                try:
                    category_count = self.sheets_service.refresh_category_matrix_sheets(start_date=category_start_date)
                    self.logger.info(
                        "Positions category sheets refreshed: %d (start_date=%s)",
                        category_count,
                        category_start_date or "all",
                    )
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("Category sheets refresh skipped due to error: %s", exc)
            finish_iso = self._iso_now(self._now())
            self.sheets_service.mark_success(run_time=finish_iso, rows=written_rows)
            self.logger.info("Positions collector completed. upsert_rows=%d", written_rows)
            return written_rows
        except Exception as exc:
            finish_iso = self._iso_now(self._now())
            self.sheets_service.mark_error(run_time=finish_iso, error_message=str(exc))
            self.logger.exception("Positions collector failed: %s", exc)
            raise
        finally:
            self.mpstats_client.close()
            if self.wb_analytics_client:
                self.wb_analytics_client.close()
            self.wb_content_resolver.close()

    def _collect_pair(
        self,
        nm_id: int,
        user_query: str,
        is_own_brand: bool,
        collection_date: str,
    ) -> dict[str, Any]:
        if not is_own_brand:
            return self._collect_from_mpstats(nm_id=nm_id, user_query=user_query, collection_date=collection_date)

        if not self.wb_analytics_client:
            return self._collect_from_mpstats(nm_id=nm_id, user_query=user_query, collection_date=collection_date)

        wb_result: WBAnalyticsSearchResult = self.wb_analytics_client.fetch_search_result(
            nm_id=nm_id,
            query=user_query,
            target_date=collection_date,
        )
        matched_query = str(wb_result.matched_query or "").strip() or user_query
        if wb_result.status == "found":
            return {
                "position": wb_result.position,
                "organic_position": None,
                "boost_position": None,
                "matched_query": matched_query,
                "match_type": wb_result.match_type,
                "traffic_volume": wb_result.traffic_volume,
                "status": "found",
                "data_source": "wb_analytics",
                "error_msg": "",
            }

        if wb_result.status == "not_found" and not self.wb_fallback_on_not_found:
            return {
                "position": None,
                "organic_position": None,
                "boost_position": None,
                "matched_query": matched_query,
                "match_type": wb_result.match_type,
                "traffic_volume": wb_result.traffic_volume,
                "status": "not_found",
                "data_source": "wb_analytics",
                "error_msg": "",
            }

        mpstats_result = self._collect_from_mpstats(
            nm_id=nm_id,
            user_query=user_query,
            collection_date=collection_date,
        )
        if wb_result.status == "source_error" and mpstats_result["status"] == "source_error":
            if wb_result.error_msg and mpstats_result["error_msg"]:
                mpstats_result["error_msg"] = f"WB: {wb_result.error_msg}; MPSTATS: {mpstats_result['error_msg']}"
            elif wb_result.error_msg and not mpstats_result["error_msg"]:
                mpstats_result["error_msg"] = f"WB: {wb_result.error_msg}"
        return mpstats_result

    def _collect_from_mpstats(self, nm_id: int, user_query: str, collection_date: str) -> dict[str, Any]:
        result: MPStatsSearchResult = self.mpstats_client.fetch_search_result(
            nm_id=nm_id,
            query=user_query,
            target_date=collection_date,
        )
        matched_query = str(result.matched_query or "").strip() or str(user_query or "").strip()
        return {
            "position": result.position,
            "organic_position": result.organic_position,
            "boost_position": result.boost_position,
            "matched_query": matched_query,
            "match_type": result.match_type,
            "traffic_volume": result.traffic_volume,
            "status": result.status,
            "data_source": "mpstats",
            "error_msg": result.error_msg,
        }

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _now(self) -> datetime:
        return datetime.now(tz=self.timezone)

    @staticmethod
    def _iso_now(value: datetime) -> str:
        return value.isoformat(timespec="seconds")

    @staticmethod
    def _resolve_collection_dates(today: date, date_from: str = "", date_to: str = "") -> list[str]:
        """Resolve collection dates for daily or backfill mode."""
        if not date_from and not date_to:
            return [today.isoformat()]
        if date_to and not date_from:
            raise ValueError("date_to requires date_from.")

        start = date.fromisoformat(date_from) if date_from else today
        end = date.fromisoformat(date_to) if date_to else start
        if start > end:
            raise ValueError("date_from must be earlier than or equal to date_to.")

        days: list[str] = []
        current = start
        while current <= end:
            days.append(current.isoformat())
            current += timedelta(days=1)
        return days



