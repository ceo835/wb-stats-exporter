"""Automatic daily WB export script for GitHub Actions."""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

import data_processor
from google_sheets import try_save_to_google_sheets
from logger_utils import setup_logging
from wb_api import WBApiClient


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
) -> None:
    """Send telegram notification (optional)."""
    if not bot_token or not chat_id:
        return

    requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=15,
    )


def main() -> int:
    """Entry point for scheduled export."""
    load_dotenv()
    logger = setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    wb_token = os.getenv("WB_TOKEN", "").strip()
    if not wb_token:
        logger.error("WB_TOKEN is empty.")
        return 1

    target_date = date.today() - timedelta(days=2)
    start_date = target_date.isoformat()
    end_date = target_date.isoformat()
    logger.info("Auto export started for date: %s", target_date)

    client = WBApiClient(token=wb_token, logger=logger)

    try:
        client.validate_token()
        rows = client.fetch_stats_rows(start_date=start_date, end_date=end_date)
        prepared = data_processor.prepare_data(rows=rows, filter_zero_spend=True)
        raw_df = prepared["raw_df"]
        summary_df = prepared["summary_df"]

        if raw_df.empty:
            logger.warning("No rows returned for %s.", target_date)
            message = f"WB export: нет данных за {target_date}"
            send_telegram_message(
                bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
                chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
                text=message,
            )
            return 0

        ok, result = try_save_to_google_sheets(
            raw_df=raw_df,
            summary_df=summary_df,
            start_date=start_date,
            end_date=end_date,
            logger=logger,
        )
        if ok:
            logger.info("Google Sheets export succeeded: %s", result)
        else:
            logger.warning("Google Sheets export skipped/failed: %s", result)

        message = (
            f"WB export {target_date}: успешно, строк {len(raw_df)}."
            if ok
            else f"WB export {target_date}: выгрузка без Google Sheets, строк {len(raw_df)}. {result}"
        )
        send_telegram_message(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            text=message,
        )

        logger.info("Auto export finished successfully.")
        return 0

    except Exception:
        logger.exception("Auto export failed.")
        send_telegram_message(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            text=f"WB export {target_date}: ошибка, см. logs/app.log.",
        )
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
