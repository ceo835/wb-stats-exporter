"""Entry point for background positions collection job."""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from logger_utils import setup_logging
from services.mpstats_service import MPStatsClient
from services.positions_collector import PositionsCollector
from services.positions_gsheets_service import PositionsSheetsService
from services.wb_analytics_service import WBAnalyticsClient
from services.wb_content_service import WBContentNameResolver


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run hybrid positions collector (WB Analytics + MPSTATS).")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run collector even without trigger flag.",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=0,
        help="Limit number of pairs for quick dev runs. 0 means all pairs.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logger level (DEBUG/INFO/WARNING/ERROR).",
    )
    parser.add_argument(
        "--date-from",
        type=str,
        default="",
        help="Start date for backfill in YYYY-MM-DD format. If date-to is omitted, only this date is collected.",
    )
    parser.add_argument(
        "--date-to",
        type=str,
        default="",
        help="End date for backfill in YYYY-MM-DD format (inclusive).",
    )
    return parser.parse_args()


def main() -> int:
    """Run collection flow."""
    load_dotenv()
    args = parse_args()
    logger = setup_logging(args.log_level)

    try:
        sheets_service = PositionsSheetsService.from_env(logger=logger)
        mpstats_client = MPStatsClient.from_env(logger=logger)
        wb_analytics_client = WBAnalyticsClient.from_env_optional(logger=logger)
        wb_content_resolver = WBContentNameResolver.from_env(logger=logger)
        collector = PositionsCollector(
            sheets_service=sheets_service,
            mpstats_client=mpstats_client,
            wb_analytics_client=wb_analytics_client,
            wb_content_resolver=wb_content_resolver,
            logger=logger,
        )
        written = collector.run(
            force=args.force,
            max_pairs=max(args.max_pairs, 0),
            date_from=str(args.date_from or "").strip(),
            date_to=str(args.date_to or "").strip(),
        )
        logger.info("Positions collector finished. rows=%d", written)
        return 0
    except Exception:
        logger.exception("Positions collector terminated with error.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
