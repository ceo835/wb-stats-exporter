"""Streamlit app for WB ads analytics."""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import streamlit as st
from dotenv import load_dotenv

import data_processor
from google_sheets import is_google_sheets_configured, try_save_to_google_sheets
from logger_utils import get_log_level, setup_logging
from wb_api import WBApiClient

load_dotenv()

st.set_page_config(
    page_title="Wildberries — Рекламная статистика",
    page_icon="📊",
    layout="wide",
)


TRUE_VALUES = {"1", "true", "yes", "on"}


def _load_streamlit_secrets() -> dict[str, Any]:
    """Safely load Streamlit secrets (without warnings when file is absent)."""
    local_paths = [
        Path.home() / ".streamlit" / "secrets.toml",
        Path.cwd() / ".streamlit" / "secrets.toml",
    ]
    if not any(path.exists() for path in local_paths):
        return {}

    try:
        return dict(st.secrets)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _get_setting(name: str, default: str = "") -> str:
    """Read setting from Streamlit secrets first, then env."""
    secrets = _load_streamlit_secrets()
    if name in secrets:
        return str(secrets[name]).strip()
    return os.getenv(name, default).strip()


def _sync_streamlit_secrets_to_env() -> None:
    """Mirror selected Streamlit secrets into process env for shared modules."""
    secrets = _load_streamlit_secrets()
    for key in (
        "LOG_LEVEL",
        "GOOGLE_SPREADSHEET_ID",
        "GOOGLE_CREDENTIALS_FILE",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
    ):
        if key in secrets and str(secrets[key]).strip():
            os.environ[key] = str(secrets[key]).strip()

    if "GOOGLE_CREDENTIALS_JSON" in secrets:
        raw = secrets["GOOGLE_CREDENTIALS_JSON"]
        credentials_path = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials_streamlit.json")
        target = Path(credentials_path)

        if isinstance(raw, dict):
            target.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
            os.environ["GOOGLE_CREDENTIALS_FILE"] = str(target)
        elif isinstance(raw, str) and raw.strip():
            target.write_text(raw.strip(), encoding="utf-8")
            os.environ["GOOGLE_CREDENTIALS_FILE"] = str(target)


def _get_logger() -> logging.Logger:
    """Get initialized logger."""
    log_level = _get_setting("LOG_LEVEL", get_log_level("INFO"))
    return setup_logging(log_level=log_level)


def _build_report_sheets_compat(prepared_export: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    """Build report sheets with backward compatibility for older data_processor."""
    if not prepared_export:
        return {}, []

    raw_df = prepared_export.get("raw_df")
    summary_df = prepared_export.get("summary_df")
    table_df = prepared_export.get("table_df")

    builder = getattr(data_processor, "build_report_sheets", None)
    if callable(builder):
        sheets = builder(raw_df=raw_df, summary_df=summary_df)
        order = getattr(data_processor, "REPORT_SHEET_ORDER", list(sheets.keys()))
        return sheets, list(order)

    # Fallback for older deployments where build_report_sheets is unavailable.
    sheets: dict[str, Any] = {}
    if summary_df is not None:
        sheets["Сводные"] = summary_df
    if table_df is not None:
        sheets["Данные"] = table_df
    elif raw_df is not None:
        sheets["Данные"] = raw_df
    return sheets, list(sheets.keys())


@st.cache_data(show_spinner=False, ttl=300)
def _validate_wb_token_cached(token: str, log_level: str) -> tuple[bool, str]:
    """Validate WB token with short cache."""
    logger = setup_logging(log_level)
    client = WBApiClient(token=token, logger=logger)
    try:
        client.validate_token()
        return True, ""
    except Exception as exc:  # noqa: BLE001 - user-facing status needed
        return False, str(exc)
    finally:
        client.close()


@st.cache_data(show_spinner=False, ttl=3600)
def _fetch_rows_cached(
    token: str,
    start_date: str,
    end_date: str,
    log_level: str,
    full_scan_all_campaigns: bool = False,
) -> list[dict[str, Any]]:
    """Fetch WB stats rows with Streamlit cache."""
    logger = setup_logging(log_level)
    client = WBApiClient(token=token, logger=logger)
    try:
        return client.fetch_stats_rows(
            start_date=start_date,
            end_date=end_date,
            full_scan_all_campaigns=full_scan_all_campaigns,
        )
    finally:
        client.close()


def _render_theme_css() -> None:
    """Apply light style adjustments."""
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] .main .block-container {
            padding-top: 2.2rem;
        }
        @media (max-width: 768px) {
            [data-testid="stAppViewContainer"] .main .block-container {
                padding-top: 2.8rem;
            }
        }
        .wb-status-box {
            border: 1px solid #d1d5db;
            border-radius: 10px;
            padding: 10px 14px;
            background: #f9fafb;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _init_state() -> None:
    """Initialize Streamlit session keys."""
    st.session_state.setdefault("prepared_view", None)
    st.session_state.setdefault("prepared_export", None)
    st.session_state.setdefault("raw_rows", None)
    st.session_state.setdefault("last_update", None)
    st.session_state.setdefault("last_range", ("", ""))


def _format_money(value: float) -> str:
    return f"₽ {value:,.2f}".replace(",", " ")


def _render_metrics(metrics: dict[str, Any]) -> None:
    """Render 4 KPI cards."""
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Потрачено", _format_money(float(metrics.get("spent", 0))))
    col2.metric("Клики", f"{int(metrics.get('clicks', 0)):,}".replace(",", " "))
    col3.metric("Заказы", f"{int(metrics.get('orders', 0)):,}".replace(",", " "))
    col4.metric("CTR", f"{float(metrics.get('ctr', 0)):.2f}%")


def _render_status(last_update: datetime | None) -> None:
    """Render data freshness block."""
    text = "нет данных"
    if last_update is not None:
        text = last_update.strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(
        f"<div class='wb-status-box'><b>Данные актуальны на:</b> {text}</div>",
        unsafe_allow_html=True,
    )


def main() -> None:
    _sync_streamlit_secrets_to_env()
    logger = _get_logger()
    _init_state()

    st.title("📊 Wildberries — Рекламная статистика")
    _render_theme_css()

    today = date.today()
    default_start = today - timedelta(days=6)
    default_end = today

    show_full_scan_option = _get_setting("SHOW_FULL_SCAN_OPTION", "0").lower() in TRUE_VALUES

    if show_full_scan_option:
        col_filter_1, col_filter_2, col_filter_3, col_filter_4, col_filter_5, col_filter_6, col_filter_7 = st.columns(
            [1, 1, 1.1, 1.25, 1.05, 1.1, 1.55]
        )
    else:
        col_filter_1, col_filter_2, col_filter_3, col_filter_4, col_filter_5, col_filter_6 = st.columns(
            [1, 1, 1.2, 1.3, 1.2, 1.2]
        )
    with col_filter_1:
        start_date = st.date_input("Дата начала", value=default_start, format="YYYY-MM-DD")
    with col_filter_2:
        end_date = st.date_input("Дата окончания", value=default_end, format="YYYY-MM-DD")
    with col_filter_3:
        filter_zero_spend = st.checkbox("Скрыть нулевые товары (только KPI/графики)", value=False)
    with col_filter_4:
        st.caption("Таблицы: Кампании / Товары / Товары детально")
    with col_filter_5:
        st.caption("Товары: агрегировано и детально (отдельные вкладки)")
    with col_filter_6:
        st.caption("Таблицы и Excel всегда включают нулевые товары (как в WB).")
    full_scan_all_campaigns = False
    if show_full_scan_option:
        with col_filter_7:
            full_scan_all_campaigns = st.checkbox(
                "Полный скан РК (медленно)",
                value=False,
                help=(
                    "Запрашивает статистику по всем кампаниям без фильтра статуса. "
                    "Режим дольше, чаще получает 429, но дает максимально полное покрытие."
                ),
            )

    if start_date > end_date:
        st.error("Дата начала не может быть больше даты окончания.")
        return

    if full_scan_all_campaigns:
        days = (end_date - start_date).days + 1
        st.warning(
            "Включен полный скан кампаний. Для больших периодов (например, месяц) "
            f"загрузка может занять несколько минут. Выбранный период: {days} дн."
        )

    token = _get_setting("WB_TOKEN", "")
    if not token:
        st.error("WB_TOKEN не найден. Добавьте его в `.env` или Streamlit Secrets.")
        return

    valid, validate_message = _validate_wb_token_cached(
        token=token,
        log_level=_get_setting("LOG_LEVEL", "INFO"),
    )
    if not valid:
        st.error(f"Ошибка валидации WB токена: {validate_message}")
        return

    actions_col1, actions_col2, actions_col3 = st.columns([1.2, 1.2, 1.2])
    load_clicked = actions_col1.button("🔄 Загрузить данные", type="primary", use_container_width=True)

    if load_clicked:
        with st.spinner("Загрузка данных из Wildberries..."):
            try:
                raw_rows = _fetch_rows_cached(
                    token=token,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    log_level=_get_setting("LOG_LEVEL", "INFO"),
                    full_scan_all_campaigns=full_scan_all_campaigns,
                )
                st.session_state["raw_rows"] = raw_rows
                st.session_state["last_update"] = datetime.now()
                st.session_state["last_range"] = (start_date.isoformat(), end_date.isoformat())
                logger.info(
                    "Данные загружены через UI: %s - %s, строк: %d, full_scan=%s",
                    start_date.isoformat(),
                    end_date.isoformat(),
                    len(raw_rows),
                    full_scan_all_campaigns,
                )
            except Exception as exc:  # noqa: BLE001 - display user error
                logger.exception("Ошибка загрузки данных из WB.")
                st.error(f"Ошибка загрузки: {exc}")

    raw_rows = st.session_state.get("raw_rows")
    if raw_rows is not None:
        prepared_view = data_processor.prepare_data(
            rows=raw_rows,
            filter_zero_spend=filter_zero_spend,
        )
        st.session_state["prepared_view"] = prepared_view

        prepared_export = data_processor.prepare_data(
            rows=raw_rows,
            filter_zero_spend=False,
            table_mode="all",
            aggregate_items=False,
        )
        st.session_state["prepared_export"] = prepared_export

        for warning in prepared_view.get("warnings", []):
            logger.warning(warning)

    prepared = st.session_state.get("prepared_view")
    prepared_export = st.session_state.get("prepared_export")
    last_update = st.session_state.get("last_update")
    _render_status(last_update)

    if not prepared or prepared["raw_df"].empty:
        st.info("Нажмите «🔄 Загрузить данные», чтобы получить статистику.")
    else:
        metrics = prepared["metrics"]
        _render_metrics(metrics)

        start_used, end_used = st.session_state.get("last_range", ("", ""))
        export_raw_df = prepared_export["raw_df"] if prepared_export else prepared["raw_df"]
        export_summary_df = prepared_export["summary_df"] if prepared_export else prepared["summary_df"]
        report_sheets, report_sheet_order = _build_report_sheets_compat(prepared_export)
        excel_bytes, excel_name = data_processor.build_excel_report(
            raw_df=export_raw_df,
            summary_df=export_summary_df,
            start_date=start_used,
            end_date=end_used,
        )
        actions_col2.download_button(
            "📥 Скачать Excel",
            data=excel_bytes,
            file_name=excel_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        save_disabled = not is_google_sheets_configured()
        save_clicked = actions_col3.button(
            "☁️ Сохранить в облако",
            disabled=save_disabled,
            use_container_width=True,
        )
        if save_disabled:
            actions_col3.caption("Google Sheets не настроен.")

        if save_clicked:
            with st.spinner("Сохранение в Google Sheets..."):
                ok, message = try_save_to_google_sheets(
                    raw_df=export_raw_df,
                    summary_df=export_summary_df,
                    start_date=start_used,
                    end_date=end_used,
                    logger=logger,
                )
                if ok:
                    st.success(message)
                else:
                    st.warning(f"Не удалось сохранить в облако: {message}")

        config = {
            "displaylogo": False,
            "toImageButtonOptions": {"format": "png", "filename": "wb_chart", "scale": 2},
        }
        st.subheader("Графики")
        chart_col_1, chart_col_2 = st.columns(2)

        with chart_col_1:
            st.plotly_chart(
                data_processor.build_spend_trend_chart(prepared["campaign_daily_df"], dark_mode=False),
                use_container_width=True,
                config=config,
            )
        with chart_col_2:
            st.plotly_chart(
                data_processor.build_top_campaigns_chart(prepared["summary_df"], dark_mode=False),
                use_container_width=True,
                config=config,
            )

        st.plotly_chart(
            data_processor.build_funnel_chart(prepared["campaign_daily_df"], dark_mode=False),
            use_container_width=True,
            config=config,
        )

        st.subheader("Таблицы (как в Excel)")
        campaign_filter_source = report_sheets.get("Кампании")
        selected_campaigns: list[str] = []
        if campaign_filter_source is None:
            campaign_filter_source = report_sheets.get("Данные")

        if campaign_filter_source is not None and not campaign_filter_source.empty:
            if "ID кампании" in campaign_filter_source.columns:
                campaign_series = campaign_filter_source["ID кампании"]
            elif "campaign_id" in campaign_filter_source.columns:
                campaign_series = campaign_filter_source["campaign_id"]
            else:
                campaign_series = None

            if campaign_series is not None:
                campaign_ids = campaign_series.dropna().astype(str).drop_duplicates().tolist()
            else:
                campaign_ids = []

        else:
            campaign_ids = []

        if campaign_ids:
            selected_campaigns = st.multiselect(
                "Фильтр по кампаниям",
                options=campaign_ids,
                default=[],
            )

        if not report_sheet_order:
            st.info("Нет данных.")
            return

        tabs = st.tabs(report_sheet_order)
        for tab, sheet_name in zip(tabs, report_sheet_order):
            with tab:
                table_df = report_sheets.get(sheet_name)
                if table_df is None or table_df.empty:
                    st.info("Нет данных.")
                    continue
                filtered_df = table_df
                if selected_campaigns:
                    if "ID кампании" in filtered_df.columns:
                        filtered_df = filtered_df[filtered_df["ID кампании"].astype(str).isin(selected_campaigns)].copy()
                    elif "campaign_id" in filtered_df.columns:
                        filtered_df = filtered_df[filtered_df["campaign_id"].astype(str).isin(selected_campaigns)].copy()
                st.dataframe(filtered_df, use_container_width=True, hide_index=True)




if __name__ == "__main__":
    main()


