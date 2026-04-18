"""Models for search positions collection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class PositionPair:
    """Input pair (nm_id + query) to collect."""

    nm_id: int
    user_query: str
    product_name: str = ""
    is_own_brand: bool = False
    source: str = "settings_sheet"


@dataclass
class PositionRecord:
    """Single collected position row."""

    date: str
    collected_at: str
    nm_id: int
    product_name: str
    user_query: str
    matched_query: str = ""
    match_type: str = ""
    position: Optional[int] = None
    organic_position: Optional[int] = None
    boost_position: Optional[int] = None
    traffic_volume: Optional[int] = None
    status: str = "not_found"
    data_source: str = "mpstats"
    error_msg: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Convert model to plain dictionary."""
        return {
            "date": self.date,
            "collected_at": self.collected_at,
            "nm_id": self.nm_id,
            "product_name": self.product_name,
            "user_query": self.user_query,
            "matched_query": self.matched_query,
            "match_type": self.match_type,
            "position": self.position,
            "organic_position": self.organic_position,
            "boost_position": self.boost_position,
            "traffic_volume": self.traffic_volume,
            "status": self.status,
            "data_source": self.data_source,
            "error_msg": self.error_msg,
        }


@dataclass
class CollectorState:
    """Collector state stored in Sheets state key-value table."""

    trigger_pending: bool = False
    trigger_requested_at: str = ""
    last_run_status: str = ""
    last_run_time: str = ""
    running: bool = False
    last_error: str = ""
    last_run_rows: int = 0

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        return text in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def from_mapping(cls, values: dict[str, str]) -> "CollectorState":
        """Build state from key-value mapping."""
        return cls(
            trigger_pending=cls._to_bool(values.get("trigger_pending", "")),
            trigger_requested_at=values.get("trigger_requested_at", ""),
            last_run_status=values.get("last_run_status", ""),
            last_run_time=values.get("last_run_time", ""),
            running=cls._to_bool(values.get("running", "")),
            last_error=values.get("last_error", ""),
            last_run_rows=cls._to_int(values.get("last_run_rows", 0)),
        )

    def as_mapping(self) -> dict[str, str]:
        """Convert state to string mapping for key-value storage."""
        return {
            "trigger_pending": "1" if self.trigger_pending else "0",
            "trigger_requested_at": self.trigger_requested_at,
            "last_run_status": self.last_run_status,
            "last_run_time": self.last_run_time,
            "running": "1" if self.running else "0",
            "last_error": self.last_error,
            "last_run_rows": str(self.last_run_rows),
        }
