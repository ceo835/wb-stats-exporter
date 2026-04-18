"""Streamlit app for WB ads and search positions analytics."""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

import data_processor
from google_sheets import is_google_sheets_configured, try_save_to_google_sheets
from logger_utils import get_log_level, setup_logging
from services.positions_groups import POSITION_CATEGORY_ORDER, POSITION_CATEGORY_OTHER, classify_position_category
from services.positions_models import CollectorState
from services.positions_gsheets_service import PositionsSheetsService
from wb_api import WBApiClient

load_dotenv(override=True)

st.set_page_config(
    page_title="Wildberries — Аналитика",
    page_icon="📊",
    layout="wide",
)

TABLE_MODE_LABEL_TO_VALUE = {
    "Только итоги кампаний": "totals",
    "Только товары": "items",
    "Все строки": "all",
}
TRUE_VALUES = {"1", "true", "yes", "on"}
POS_TRIGGER_COOLDOWN_SECONDS = 60
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
RUN_STATUS_LABELS = {
    "success": "Успешно",
    "running": "Выполняется",
    "error": "Ошибка",
}
ROW_STATUS_LABELS = {
    "found": "✅ Найдено",
    "not_found": "❌ Не найдено",
    "source_error": "⚠️ Ошибка источника",
}
DATA_SOURCE_LABELS = {
    "wb_analytics": "WB Analytics",
    "mpstats": "MPSTATS",
    "unknown": "Не определен",
}
MATCH_TYPE_LABELS = {
    "exact": "Точное совпадение",
    "norm_query": "Нормализованное совпадение",
    "partial": "Частичное совпадение",
    "best_position": "Лучшая позиция",
    "not_found": "Совпадение не найдено",
}


def _format_user_date(value: Any) -> str:
    """Convert date to user-friendly DD.MM.YYYY format."""
    text = str(value or "").strip()
    if not text:
        return "—"
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return text


def _format_user_datetime(value: Any) -> str:
    """Convert datetime to user-friendly format for Moscow timezone."""
    text = str(value or "").strip()
    if not text:
        return "—"

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%d.%m.%Y %H:%M")
        except ValueError:
            return text

    if dt.tzinfo is not None:
        dt = dt.astimezone(MOSCOW_TZ)
        return f"{dt.strftime('%d.%m.%Y %H:%M')} МСК"
    return dt.strftime("%d.%m.%Y %H:%M")


def _map_run_status(status: str, running: bool = False) -> str:
    """Map technical collector status to user-friendly label."""
    if running:
        return RUN_STATUS_LABELS["running"]
    key = str(status or "").strip().lower()
    if not key:
        return "Нет данных"
    return RUN_STATUS_LABELS.get(key, key)


def _map_row_status(status: Any) -> str:
    """Map row status to user-friendly label."""
    key = str(status or "").strip().lower()
    if not key:
        return "—"
    return ROW_STATUS_LABELS.get(key, key)


def _map_data_source(source: Any) -> str:
    """Map factual data source to user-friendly label."""
    key = str(source or "").strip().lower()
    if not key:
        return "—"
    return DATA_SOURCE_LABELS.get(key, key)


def _format_traffic(value: Any, data_source: Any) -> str:
    """Human-readable traffic value."""
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return "—"
    number = int(parsed)
    source_key = str(data_source or "").strip().lower()
    if source_key == "mpstats":
        return f"{number} (MPSTATS)"
    return str(number)


def _map_match_type(value: Any) -> str:
    """Map technical match type to user-friendly text."""
    key = str(value or "").strip().lower()
    if not key:
        return "—"
    return MATCH_TYPE_LABELS.get(key, key)


def _format_row_error(status: Any, data_source: Any, error_msg: Any) -> str:
    """Render user-facing error text without technical payload details."""
    status_key = str(status or "").strip().lower()
    source_label = _map_data_source(data_source)
    if status_key == "source_error":
        return f"Временная ошибка источника ({source_label}). Повторите проверку позже."

    text = str(error_msg or "").strip()
    if not text:
        return "—"
    if len(text) > 120:
        return f"{text[:117]}..."
    return text


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
        "GOOGLE_CREDENTIALS_JSON",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "MPSTATS_API_TOKEN",
        "MPSTATS_POSITION_URL",
        "WB_ANALYTICS_TOKEN",
        "WB_ANALYTICS_POSITION_URL",
        "WB_FALLBACK_ON_NOT_FOUND",
        "WB_CONTENT_TOKEN",
        "WB_CONTENT_URL",
        "MY_NM_IDS",
        "POSITIONS_MY_NM_IDS",
        "POSITIONS_NM_CACHE_FILE",
        "POSITIONS_SETTINGS_SHEET",
        "POSITIONS_RAW_SHEET",
        "POSITIONS_STATE_SHEET",
        "POSITIONS_SETTINGS_CSV",
        "POSITIONS_TIMEZONE",
        "POSITIONS_REQUEST_PAUSE_SECONDS",
        "POSITIONS_MATRIX_ENABLED",
        "POSITIONS_MATRIX_FROM_MONTH_START",
        "POSITIONS_MATRIX_INDEX_SHEET",
        "POSITIONS_MATRIX_SHEET_PREFIX",
        "POSITIONS_CATEGORY_MATRIX_ENABLED",
        "POSITIONS_CATEGORY_INDEX_SHEET",
        "POSITIONS_CATEGORY_SHEET_PREFIX",
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


@st.cache_data(show_spinner=False, ttl=120)
def _load_positions_state_cached(
    log_level: str,
    spreadsheet_id: str,
    state_sheet: str,
    raw_sheet: str,
) -> dict[str, str]:
    """Read positions collector state from Google Sheets."""
    logger = setup_logging(log_level)
    service = PositionsSheetsService.from_env(logger=logger)
    service.ensure_base_sheets()
    return service.get_collector_state().as_mapping()


@st.cache_data(show_spinner=False, ttl=120)
def _load_positions_rows_cached(
    log_level: str,
    spreadsheet_id: str,
    state_sheet: str,
    raw_sheet: str,
) -> list[dict[str, Any]]:
    """Read raw positions rows from Google Sheets."""
    logger = setup_logging(log_level)
    service = PositionsSheetsService.from_env(logger=logger)
    service.ensure_base_sheets()
    frame = service.load_positions_raw()
    return frame.to_dict(orient="records")


def _positions_cache_context() -> tuple[str, str, str]:
    """Return current Sheets context so Streamlit cache invalidates on config change."""
    return (
        _get_setting("GOOGLE_SPREADSHEET_ID", ""),
        _get_setting("POSITIONS_STATE_SHEET", "Positions_State"),
        _get_setting("POSITIONS_RAW_SHEET", "Positions_Raw"),
    )


def _request_positions_trigger(log_level: str) -> CollectorState:
    """Set positions trigger flag (without running collector in UI session)."""
    logger = setup_logging(log_level)
    service = PositionsSheetsService.from_env(logger=logger)
    service.ensure_base_sheets()
    now_iso = datetime.now(tz=MOSCOW_TZ).isoformat(timespec="seconds")
    return service.request_trigger(requested_at=now_iso)


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
        .pos-status-card {
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 8px 10px;
            background: #ffffff;
            min-height: 58px;
        }
        .pos-status-label {
            font-size: 0.64rem;
            color: #6b7280;
            margin-bottom: 3px;
            line-height: 1.2;
        }
        .pos-status-value {
            font-size: 0.78rem;
            font-weight: 600;
            color: #111827;
            line-height: 1.25;
            word-break: break-word;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _init_state() -> None:
    """Initialize Streamlit session keys."""
    st.session_state.setdefault("prepared_view", None)
    st.session_state.setdefault("raw_rows", None)
    st.session_state.setdefault("last_update", None)
    st.session_state.setdefault("last_range", ("", ""))
    st.session_state.setdefault("pos_trigger_disabled_until_ts", 0.0)


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


def _render_ads_tab(logger: logging.Logger) -> None:
    """Render existing WB ads analytics tab."""
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
        start_date = st.date_input("Дата начала", value=default_start, format="YYYY-MM-DD", key="ad_start_date")
    with col_filter_2:
        end_date = st.date_input("Дата окончания", value=default_end, format="YYYY-MM-DD", key="ad_end_date")
    with col_filter_3:
        filter_zero_spend = st.checkbox("Скрыть нулевые товары (экран)", value=False, key="ad_filter_zero_spend")
    with col_filter_4:
        table_mode_labels = list(TABLE_MODE_LABEL_TO_VALUE.keys())
        table_mode_label = st.selectbox(
            "Таблица",
            table_mode_labels,
            index=table_mode_labels.index("Все строки"),
            key="ad_table_mode",
        )
        table_mode = TABLE_MODE_LABEL_TO_VALUE[table_mode_label]
    with col_filter_5:
        aggregate_items = st.checkbox("Объединять товары", value=True, key="ad_aggregate_items")
    with col_filter_6:
        st.caption("Экспорт и облако: как на экране (те же фильтры).")

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
                key="ad_full_scan_all_campaigns",
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
    load_clicked = actions_col1.button("🔄 Загрузить данные", type="primary", use_container_width=True, key="ad_load")

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
            table_mode=table_mode,
            aggregate_items=aggregate_items,
        )
        st.session_state["prepared_view"] = prepared_view
        for warning in prepared_view.get("warnings", []):
            logger.warning(warning)

    prepared = st.session_state.get("prepared_view")
    last_update = st.session_state.get("last_update")
    _render_status(last_update)

    if not prepared or prepared["raw_df"].empty:
        st.info("Нажмите «🔄 Загрузить данные», чтобы получить статистику.")
        return

    metrics = prepared["metrics"]
    _render_metrics(metrics)

    start_used, end_used = st.session_state.get("last_range", ("", ""))
    export_raw_df = prepared["raw_df"]
    export_summary_df = prepared["summary_df"]
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
        key="ad_download_excel",
    )

    save_disabled = not is_google_sheets_configured()
    save_clicked = actions_col3.button(
        "☁️ Сохранить в облако",
        disabled=save_disabled,
        use_container_width=True,
        key="ad_save_cloud",
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

    st.subheader("Таблица данных")
    table_df = prepared["table_df"].copy()

    if not table_df.empty and "ID кампании" in table_df.columns:
        campaign_ids = table_df["ID кампании"].dropna().astype(str).drop_duplicates().tolist()
        selected_campaigns = st.multiselect(
            "Фильтр по кампаниям",
            options=campaign_ids,
            default=[],
            key="ad_selected_campaigns",
        )
        if selected_campaigns:
            table_df = table_df[table_df["ID кампании"].astype(str).isin(selected_campaigns)].copy()

    st.dataframe(table_df, use_container_width=True, hide_index=True)


def _render_positions_status(state: CollectorState) -> None:
    """Render status panel for positions collector."""
    status_label = _map_run_status(state.last_run_status, running=bool(state.running))
    formatted_last_run = _format_user_datetime(state.last_run_time)

    if state.last_run_status == "success":
        timestamp = formatted_last_run
        st.success(f"✅ Данные актуальны. Последнее обновление: {timestamp}")
    elif state.running or state.last_run_status == "running":
        st.info("⏳ Сбор данных в процессе... Обновите страницу через 2 мин.")
    else:
        st.warning("⚠️ Данные устарели. Нажмите «Запустить проверку».")

    trigger_label = "Ожидает запуска" if state.trigger_pending else "Нет"
    status_items = [
        ("Статус", status_label),
        ("Последний запуск", formatted_last_run),
        ("Обновлено строк", f"{int(state.last_run_rows):,}".replace(",", " ")),
        ("Триггер", trigger_label),
    ]
    status_cols = st.columns(4)
    for index, (label, value) in enumerate(status_items):
        status_cols[index].markdown(
            (
                "<div class='pos-status-card'>"
                f"<div class='pos-status-label'>{label}</div>"
                f"<div class='pos-status-value'>{value}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    if state.last_error:
        st.caption("Последний запуск завершился с ошибкой источника данных.")
        with st.expander("Детали для поддержки"):
            st.code(state.last_error)


def _prepare_positions_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize positions rows for UI rendering."""
    if not rows:
        return pd.DataFrame(
            columns=[
                "date",
                "collected_at",
                "nm_id",
                "product_name",
                "user_query",
                "matched_query",
                "match_type",
                "position",
                "organic_position",
                "boost_position",
                "traffic_volume",
                "status",
                "data_source",
                "error_msg",
                "position_previous",
                "position_change",
                "category",
            ]
        )

    frame = pd.DataFrame(rows).copy()
    if "user_query" not in frame.columns and "query" in frame.columns:
        frame["user_query"] = frame["query"]
    if "error_msg" not in frame.columns and "error" in frame.columns:
        frame["error_msg"] = frame["error"]
    if "product_name" not in frame.columns:
        frame["product_name"] = ""
    if "matched_query" not in frame.columns:
        frame["matched_query"] = frame.get("user_query", "")
    if "match_type" not in frame.columns:
        frame["match_type"] = "exact"
    if "organic_position" not in frame.columns:
        frame["organic_position"] = pd.NA
    if "boost_position" not in frame.columns:
        frame["boost_position"] = frame.get("position", pd.NA)
    if "traffic_volume" not in frame.columns:
        frame["traffic_volume"] = pd.NA

    for column in ("date", "collected_at", "product_name", "user_query", "matched_query", "match_type", "status", "data_source", "error_msg"):
        if column not in frame.columns:
            frame[column] = ""
        frame[column] = frame[column].fillna("").astype(str).str.strip()

    frame["status"] = frame["status"].str.lower().replace(
        {
            "ok": "found",
            "error": "source_error",
        }
    )
    frame.loc[frame["status"] == "", "status"] = "not_found"
    frame["data_source"] = frame["data_source"].str.lower()
    frame.loc[frame["data_source"] == "", "data_source"] = "unknown"

    frame["nm_id"] = pd.to_numeric(frame.get("nm_id"), errors="coerce").astype("Int64")
    frame["position"] = pd.to_numeric(frame.get("position"), errors="coerce").astype("Int64")
    frame["organic_position"] = pd.to_numeric(frame.get("organic_position"), errors="coerce").astype("Int64")
    frame["boost_position"] = pd.to_numeric(frame.get("boost_position"), errors="coerce").astype("Int64")
    frame["traffic_volume"] = pd.to_numeric(frame.get("traffic_volume"), errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["nm_id"]).copy()
    frame["nm_id"] = frame["nm_id"].astype(int)
    frame["category"] = frame.apply(
        lambda row: classify_position_category(
            row.get("product_name", ""),
            row.get("user_query", ""),
            row.get("matched_query", ""),
        ),
        axis=1,
    )

    history = frame.sort_values(by=["nm_id", "user_query", "date", "collected_at"], ascending=[True, True, True, True]).copy()
    history["position_previous"] = history.groupby(["nm_id", "user_query"])["position"].shift(1)
    history["position_change"] = history["position"] - history["position_previous"]
    history["position_previous"] = pd.to_numeric(history["position_previous"], errors="coerce").astype("Int64")
    history["position_change"] = pd.to_numeric(history["position_change"], errors="coerce").astype("Int64")

    history = history.sort_values(by=["date", "collected_at", "nm_id", "user_query"], ascending=[False, False, True, True])
    return history.reset_index(drop=True)


def _render_positions_tab(logger: logging.Logger) -> None:
    """Render positions UI tab (reads Sheets only, no direct MPSTATS calls)."""
    log_level = _get_setting("LOG_LEVEL", "INFO")
    spreadsheet_id, state_sheet_name, raw_sheet_name = _positions_cache_context()
    st.subheader("Позиции в поиске")

    if not is_google_sheets_configured():
        st.warning("Google Sheets не настроен. Для вкладки «Позиции» требуется доступ к таблице.")
        return

    refresh_col, trigger_col = st.columns([1.0, 1.2])
    if refresh_col.button("Обновить графики", key="pos_refresh"):
        _load_positions_rows_cached.clear()
        _load_positions_state_cached.clear()
        st.rerun()

    now_ts = datetime.now(tz=MOSCOW_TZ).timestamp()
    disabled_until_ts = float(st.session_state.get("pos_trigger_disabled_until_ts", 0.0))
    cooldown_left = int(max(0.0, disabled_until_ts - now_ts))

    try:
        state = CollectorState.from_mapping(
            _load_positions_state_cached(log_level, spreadsheet_id, state_sheet_name, raw_sheet_name)
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка чтения состояния коллектора позиций.")
        st.error(f"Не удалось загрузить статус коллектора: {exc}")
        return

    trigger_disabled = state.running or cooldown_left > 0
    trigger_help = "Ожидайте завершения текущего запуска." if state.running else ""
    if cooldown_left > 0:
        trigger_help = f"Повторный запуск будет доступен через {cooldown_left} сек."

    if trigger_col.button(
        "Запустить проверку",
        disabled=trigger_disabled,
        use_container_width=True,
        key="pos_trigger",
        help=trigger_help or None,
    ):
        try:
            _request_positions_trigger(log_level)
            st.session_state["pos_trigger_disabled_until_ts"] = now_ts + POS_TRIGGER_COOLDOWN_SECONDS
            _load_positions_state_cached.clear()
            _load_positions_rows_cached.clear()
            st.success("✅ Запрос на сбор отправлен. Данные обновятся в течение 5–10 минут. Не закрывайте вкладку.")
            st.rerun()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка отправки trigger на сбор позиций.")
            st.error(f"Не удалось отправить trigger: {exc}")
            return

    _render_positions_status(state)

    try:
        raw_rows = _load_positions_rows_cached(log_level, spreadsheet_id, state_sheet_name, raw_sheet_name)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Ошибка чтения сырых позиций.")
        st.error(f"Не удалось загрузить данные позиций: {exc}")
        return

    frame = _prepare_positions_dataframe(raw_rows)
    if frame.empty:
        st.info("Данных позиций пока нет. Нажмите «Запустить проверку» и дождитесь завершения коллектора.")
        return

    frame["date_dt"] = pd.to_datetime(frame["date"], errors="coerce")
    min_date = frame["date_dt"].min()
    max_date = frame["date_dt"].max()
    if pd.isna(min_date) or pd.isna(max_date):
        min_date = date.today() - timedelta(days=7)
        max_date = date.today()
    else:
        min_date = min_date.date()
        max_date = max_date.date()

    filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns([1.2, 1.0, 1.0, 1.1])
    with filter_col_1:
        date_range = st.date_input(
            "Период",
            value=(min_date, max_date),
            format="YYYY-MM-DD",
            key="pos_date_range",
        )
    with filter_col_2:
        nm_options = sorted(frame["nm_id"].dropna().astype(int).unique().tolist())
        selected_nm = st.multiselect("Артикул", options=nm_options, default=[], key="pos_selected_nm")
    with filter_col_3:
        query_options = sorted(frame["user_query"].dropna().astype(str).unique().tolist())
        selected_queries = st.multiselect("Запрос", options=query_options, default=[], key="pos_selected_query")
    with filter_col_4:
        category_options = POSITION_CATEGORY_ORDER + [POSITION_CATEGORY_OTHER]
        selected_categories = st.multiselect(
            "Категория",
            options=category_options,
            default=[],
            key="pos_selected_category",
        )

    filtered = frame.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        if start_date and end_date:
            filtered = filtered[filtered["date"].between(start_date.isoformat(), end_date.isoformat())]
    elif isinstance(date_range, date):
        filtered = filtered[filtered["date"] == date_range.isoformat()]

    if selected_nm:
        filtered = filtered[filtered["nm_id"].isin(selected_nm)]
    if selected_queries:
        selected_set = {query.strip() for query in selected_queries}
        filtered = filtered[filtered["user_query"].isin(selected_set)]
    if selected_categories:
        filtered = filtered[filtered["category"].isin(selected_categories)]

    if filtered.empty:
        st.info("Нет строк по выбранным фильтрам.")
        return

    hide_unknown_rows = st.checkbox(
        "Скрывать строки без источника данных",
        value=True,
        key="pos_hide_unknown_rows",
    )
    if hide_unknown_rows:
        known_sources = {"mpstats", "wb_analytics"}
        unknown_rows = int((~filtered["data_source"].isin(known_sources)).sum())
        filtered = filtered[filtered["data_source"].isin(known_sources)].copy()
        if unknown_rows > 0:
            st.caption(f"Скрыто строк без источника: {unknown_rows}")
        if filtered.empty:
            st.info("После скрытия строк без источника данных записей не осталось.")
            return

    latest_snapshot = (
        filtered.sort_values(by=["date", "collected_at"])
        .groupby(["nm_id", "user_query"], as_index=False)
        .tail(1)
        .copy()
    )
    latest_snapshot_date = ""
    if not latest_snapshot.empty:
        latest_snapshot_date = str(latest_snapshot["date"].max())

    metrics_latest_only = st.checkbox(
        "KPI только по последнему замеру",
        value=True,
        key="pos_metrics_latest_only",
    )
    metrics_frame = latest_snapshot if metrics_latest_only else filtered

    if metrics_latest_only and latest_snapshot_date:
        st.caption(f"KPI считаются по последнему замеру: {_format_user_date(latest_snapshot_date)}")

    total_pairs = len(metrics_frame)
    found_count = int((metrics_frame["status"] == "found").sum())
    not_found_count = int((metrics_frame["status"] == "not_found").sum())
    source_error_count = int((metrics_frame["status"] == "source_error").sum())
    top10_count = int((metrics_frame["position"].notna() & (metrics_frame["position"] <= 10)).sum())
    avg_position = metrics_frame["position"].dropna().mean()
    avg_position_text = "—" if pd.isna(avg_position) else f"{float(avg_position):.1f}"
    found_pct = 0.0 if total_pairs == 0 else (found_count / total_pairs) * 100.0
    not_found_pct = 0.0 if total_pairs == 0 else (not_found_count / total_pairs) * 100.0

    summary_cols = st.columns(5)
    summary_cols[0].metric("Всего строк", f"{total_pairs:,}".replace(",", " "))
    summary_cols[1].metric("✅ Найдено", f"{found_count:,} ({found_pct:.0f}%)".replace(",", " "))
    summary_cols[2].metric("❌ Не найдено", f"{not_found_count:,} ({not_found_pct:.0f}%)".replace(",", " "))
    summary_cols[3].metric("⚠️ Ошибки источника", f"{source_error_count:,}".replace(",", " "))
    summary_cols[4].metric("🏆 Топ-10 / Средняя", f"{top10_count} / {avg_position_text}")

    show_recommendations = st.checkbox(
        "Показывать подсказки по последнему замеру",
        value=False,
        key="pos_show_recommendations",
    )
    if show_recommendations:
        recommendations: list[str] = []
        latest_not_found_count = int((latest_snapshot["status"] == "not_found").sum())
        latest_source_error_count = int((latest_snapshot["status"] == "source_error").sum())
        latest_fallen_count = int((latest_snapshot["position_change"] > 0).sum())

        if latest_not_found_count > 0:
            recommendations.append(
                f"{latest_not_found_count} строк не найдено в выдаче: проверьте остатки, название и ключевые слова."
            )
        if latest_source_error_count > 0:
            recommendations.append(
                f"{latest_source_error_count} строк с ошибкой источника: запустите проверку повторно позже."
            )
        if latest_fallen_count > 0:
            recommendations.append(
                f"{latest_fallen_count} строк ухудшили позицию относительно прошлого замера."
            )

        if recommendations:
            date_label = _format_user_date(latest_snapshot_date) if latest_snapshot_date else "последний замер"
            st.info(f"Подсказки ({date_label}):\n- " + "\n- ".join(recommendations))
        else:
            st.success("По последнему замеру критичных отклонений не найдено.")

    table_frame = filtered.copy()
    table_frame["date_display"] = table_frame["date"].apply(_format_user_date)
    table_frame["collected_at_display"] = table_frame["collected_at"].apply(_format_user_datetime)
    table_frame["position_display"] = table_frame["position"].apply(lambda value: "—" if pd.isna(value) else int(value))
    table_frame["organic_display"] = table_frame["organic_position"].apply(
        lambda value: "—" if pd.isna(value) else int(value)
    )
    table_frame["boost_display"] = table_frame["boost_position"].apply(
        lambda value: "—" if pd.isna(value) else int(value)
    )
    table_frame["position_numeric"] = pd.to_numeric(table_frame["position"], errors="coerce")
    table_frame["position_bucket"] = table_frame["position_numeric"].apply(
        lambda value: "🔴 51+ / не найдено"
        if pd.isna(value) or int(value) > 50
        else ("🟡 11-50" if int(value) > 10 else "🟢 1-10")
    )
    table_frame["product_name_display"] = table_frame["product_name"].fillna("").astype(str).str.strip()
    table_frame.loc[table_frame["product_name_display"] == "", "product_name_display"] = "—"
    table_frame["category_display"] = table_frame["category"].fillna("").astype(str).str.strip()
    table_frame.loc[table_frame["category_display"] == "", "category_display"] = POSITION_CATEGORY_OTHER
    table_frame["user_query_display"] = table_frame["user_query"].fillna("").astype(str).str.strip()
    table_frame["matched_query_display"] = table_frame["matched_query"].fillna("").astype(str).str.strip()
    table_frame.loc[table_frame["matched_query_display"] == "", "matched_query_display"] = table_frame["user_query_display"]
    table_frame["match_type_display"] = table_frame["match_type"].apply(_map_match_type)
    table_frame["traffic_display"] = table_frame.apply(
        lambda row: _format_traffic(row.get("traffic_volume"), row.get("data_source")),
        axis=1,
    )
    table_frame["source_display"] = table_frame["data_source"].apply(_map_data_source)
    table_frame["status_display"] = table_frame["status"].apply(_map_row_status)
    table_frame["error_display"] = table_frame.apply(
        lambda row: _format_row_error(row.get("status"), row.get("data_source"), row.get("error_msg")),
        axis=1,
    )
    table_frame = table_frame.rename(
        columns={
            "date_display": "Дата",
            "collected_at_display": "Собрано",
            "nm_id": "Артикул",
            "category_display": "Категория",
            "product_name_display": "Товар",
            "user_query_display": "Запрос",
            "matched_query_display": "Совпавший запрос",
            "match_type_display": "Тип совпадения",
            "position_display": "Позиция",
            "organic_display": "Органика",
            "boost_display": "Буст",
            "position_bucket": "Оценка",
            "traffic_display": "Трафик",
            "source_display": "Источник данных",
            "status_display": "Статус",
            "error_display": "Ошибка",
        }
    )

    latest = filtered.sort_values(by=["date", "collected_at"]).groupby(["nm_id", "user_query"], as_index=False).tail(1)
    latest = latest.sort_values(by=["nm_id", "user_query"]).reset_index(drop=True)
    latest["category_display"] = latest["category"].fillna("").astype(str).str.strip()
    latest.loc[latest["category_display"] == "", "category_display"] = POSITION_CATEGORY_OTHER
    latest["date_display"] = latest["date"].apply(_format_user_date)
    latest["collected_at_display"] = latest["collected_at"].apply(_format_user_datetime)
    latest["position_display"] = latest["position"].apply(lambda value: "—" if pd.isna(value) else int(value))
    latest["position_previous_display"] = latest["position_previous"].apply(
        lambda value: "—" if pd.isna(value) else int(value)
    )
    latest["position_change_display"] = latest["position_change"].apply(
        lambda value: "—" if pd.isna(value) else f"{int(value):+d}"
    )
    latest_table = latest.rename(
        columns={
            "nm_id": "Артикул",
            "category_display": "Категория",
            "user_query": "Запрос",
            "date_display": "Дата",
            "collected_at_display": "Собрано",
            "position_display": "Последняя позиция",
        }
    )

    latest_delta_view = latest[
        [
            "nm_id",
            "category_display",
            "user_query",
            "date_display",
            "collected_at_display",
            "position_display",
            "position_previous_display",
            "position_change_display",
        ]
    ].copy()
    latest_delta_view = latest_delta_view.rename(
        columns={
            "nm_id": "Артикул",
            "category_display": "Категория",
            "user_query": "Запрос",
            "date_display": "Дата",
            "collected_at_display": "Собрано",
            "position_display": "Позиция",
            "position_previous_display": "Пред. позиция",
            "position_change_display": "Изменение",
        }
    )

    export_frame = table_frame[
        [
            "Дата",
            "Собрано",
            "Артикул",
            "Категория",
            "Товар",
            "Запрос",
            "Совпавший запрос",
            "Тип совпадения",
            "Позиция",
            "Органика",
            "Буст",
            "Оценка",
            "Статус",
            "Трафик",
            "Источник данных",
            "Ошибка",
        ]
    ].copy()
    with BytesIO() as output:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_frame.to_excel(writer, index=False, sheet_name="Позиции")
        positions_excel = output.getvalue()

    tab_positions, tab_example, tab_matrix = st.tabs(
        ["Позиции", "Пример внешнего вида таблицы", "Матрица позиций"]
    )

    with tab_positions:
        st.subheader("Таблица позиций")
        st.download_button(
            "📥 Скачать полный отчет Excel",
            data=positions_excel,
            file_name=f"positions_{datetime.now(tz=MOSCOW_TZ).strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
            key="pos_download_excel",
        )
        st.caption("Цвет позиции: 🟢 1-10, 🟡 11-50, 🔴 51+ или не найдено.")

        def _style_position(value: Any) -> str:
            if value == "—" or pd.isna(value):
                return "background-color: #fee2e2; color: #7f1d1d;"
            try:
                numeric = int(value)
            except (TypeError, ValueError):
                return ""
            if numeric <= 10:
                return "background-color: #dcfce7; color: #14532d;"
            if numeric <= 50:
                return "background-color: #fef9c3; color: #713f12;"
            return "background-color: #fee2e2; color: #7f1d1d;"

        display_columns = [
            "Дата",
            "Собрано",
            "Артикул",
            "Категория",
            "Товар",
            "Запрос",
            "Совпавший запрос",
            "Тип совпадения",
            "Позиция",
            "Органика",
            "Буст",
            "Оценка",
            "Статус",
            "Трафик",
            "Источник данных",
            "Ошибка",
        ]
        category_tab_labels = ["Все"] + POSITION_CATEGORY_ORDER
        category_tabs = st.tabs(category_tab_labels)
        for category_label, category_tab in zip(category_tab_labels, category_tabs):
            with category_tab:
                if category_label == "Все":
                    category_view = table_frame[display_columns].copy()
                else:
                    category_view = table_frame[table_frame["Категория"] == category_label][display_columns].copy()
                if category_view.empty:
                    st.info(f"Нет строк для категории «{category_label}».")
                else:
                    styled = category_view.style.applymap(_style_position, subset=["Позиция"])
                    st.dataframe(styled, use_container_width=True, hide_index=True)

        top_success = (
            table_frame[table_frame["Позиция"] != "—"]
            .copy()
            .sort_values(by=["Позиция", "Артикул", "Запрос"], ascending=[True, True, True])
            .head(5)
        )
        if not top_success.empty:
            st.subheader("🔥 ТОП-5 успешных строк")
            st.dataframe(
                top_success[["Артикул", "Категория", "Товар", "Запрос", "Позиция", "Трафик", "Источник данных"]],
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Сводка по последним позициям")
        st.dataframe(
            latest_table[["Артикул", "Категория", "Запрос", "Дата", "Собрано", "Последняя позиция"]],
            use_container_width=True,
            hide_index=True,
        )

        st.subheader("Динамика позиции")
        st.caption("Изменение = текущая позиция минус предыдущий замер для того же артикула и запроса.")
        st.dataframe(
            latest_delta_view[
                ["Артикул", "Категория", "Запрос", "Дата", "Собрано", "Позиция", "Пред. позиция", "Изменение"]
            ],
            use_container_width=True,
            hide_index=True,
        )

        chart_nm_options = sorted(filtered["nm_id"].dropna().astype(int).unique().tolist())
        chart_nm = st.selectbox("Артикул для графика", options=chart_nm_options, key="pos_chart_nm")
        chart_query_options = sorted(
            filtered[filtered["nm_id"] == chart_nm]["user_query"].dropna().astype(str).unique().tolist()
        )
        chart_query = st.selectbox("Запрос для графика", options=chart_query_options, key="pos_chart_query")

        chart_data = filtered[(filtered["nm_id"] == chart_nm) & (filtered["user_query"] == chart_query)].copy()
        chart_data["date_dt"] = pd.to_datetime(chart_data["date"], errors="coerce")
        chart_data = chart_data.sort_values(by=["date_dt", "collected_at"]).reset_index(drop=True)
        chart_data["position_plot"] = pd.to_numeric(chart_data["position"], errors="coerce")
        chart_data["position_plot"] = chart_data["position_plot"].fillna(101).astype(int)
        chart_data["label"] = chart_data["position_plot"].apply(lambda value: "100+" if value >= 101 else str(value))

        figure = px.line(
            chart_data,
            x="date",
            y="position_plot",
            markers=True,
            text="label",
            labels={
                "date": "Дата",
                "position_plot": "Позиция",
            },
            title=f"Позиции: {chart_nm} / {chart_query}",
        )
        figure.update_traces(textposition="top center")
        figure.update_yaxes(autorange="reversed", title="Позиция (100+ = не найдено)")
        figure.update_layout(height=420)
        st.plotly_chart(figure, use_container_width=True, config={"displaylogo": False})

    with tab_example:
        st.subheader("Пример таблицы")
        st.caption("Эталонный вид строк для листа позиций и проверки структуры колонок.")
        example = table_frame[
            [
                "Дата",
                "Собрано",
                "Артикул",
                "Категория",
                "Запрос",
                "Органика",
                "Буст",
                "Позиция",
                "Источник данных",
                "Статус",
                "Ошибка",
            ]
        ].head(1)
        if example.empty:
            example = pd.DataFrame(
                [
                    {
                        "Дата": "17.04.2026",
                        "Собрано": "17.04.2026 09:00 МСК",
                        "Артикул": 123456789,
                        "Категория": "Женские трусы (все)",
                        "Запрос": "пример запроса",
                        "Органика": 32,
                        "Буст": 14,
                        "Позиция": 14,
                        "Источник данных": "MPSTATS",
                        "Статус": "Данные получены",
                        "Ошибка": "",
                    }
                ]
            )
        st.dataframe(example, use_container_width=True, hide_index=True)

    with tab_matrix:
        st.subheader("Позиции артикула по каждому запросу")
        st.caption("Формат как в Excel: по датам, для каждого артикула пары колонок «Органика / Буст».")
        matrix_query_options = sorted(filtered["user_query"].dropna().astype(str).unique().tolist())
        if not matrix_query_options:
            st.info("Нет данных для матрицы по текущим фильтрам.")
        else:
            if len(selected_queries) == 1 and selected_queries[0] in matrix_query_options:
                default_query = selected_queries[0]
            else:
                default_query = matrix_query_options[0]

            selected_query = st.selectbox(
                "Запрос для матрицы",
                options=matrix_query_options,
                index=matrix_query_options.index(default_query),
                key="pos_matrix_query",
            )
            st.markdown(f"**Запрос:** `{selected_query}`")

            matrix_source = filtered[filtered["user_query"] == selected_query].copy()
            matrix_source = matrix_source.sort_values(by=["date", "collected_at"]).groupby(
                ["date", "nm_id"], as_index=False
            ).tail(1)
            matrix_source["organic_numeric"] = pd.to_numeric(matrix_source["organic_position"], errors="coerce")
            matrix_source["boost_numeric"] = pd.to_numeric(matrix_source["boost_position"], errors="coerce")

            organic_wide = matrix_source.pivot_table(
                index="date",
                columns="nm_id",
                values="organic_numeric",
                aggfunc="last",
            )
            boost_wide = matrix_source.pivot_table(
                index="date",
                columns="nm_id",
                values="boost_numeric",
                aggfunc="last",
            )

            all_nm_ids = sorted(
                set(organic_wide.columns.tolist()) | set(boost_wide.columns.tolist())
            )
            if not all_nm_ids:
                st.info("Нет данных для матрицы по выбранному запросу.")
            else:
                matrix_display = pd.DataFrame(index=sorted(matrix_source["date"].dropna().astype(str).unique().tolist()))
                for nm_id in all_nm_ids:
                    matrix_display[f"{nm_id} | Органика"] = organic_wide.get(nm_id, pd.Series(index=matrix_display.index))
                    matrix_display[f"{nm_id} | Буст"] = boost_wide.get(nm_id, pd.Series(index=matrix_display.index))

                matrix_display = matrix_display.sort_index()
                matrix_display.index = matrix_display.index.map(_format_user_date)
                matrix_display = matrix_display.reset_index().rename(columns={"index": "Дата"})

                numeric_cols = [column for column in matrix_display.columns if column != "Дата"]
                for column in numeric_cols:
                    matrix_display[column] = pd.to_numeric(matrix_display[column], errors="coerce")

                styled_matrix = matrix_display.style.format(
                    {column: lambda value: "—" if pd.isna(value) else int(value) for column in numeric_cols}
                )
                organic_cols = [column for column in numeric_cols if column.endswith("Органика")]
                boost_cols = [column for column in numeric_cols if column.endswith("Буст")]
                if organic_cols:
                    styled_matrix = styled_matrix.bar(
                        subset=organic_cols,
                        color="#9BB9E5",
                        align="left",
                        vmin=0,
                    )
                if boost_cols:
                    styled_matrix = styled_matrix.bar(
                        subset=boost_cols,
                        color="#A9E5C2",
                        align="left",
                        vmin=0,
                    )
                st.dataframe(styled_matrix, use_container_width=True, hide_index=True)


def main() -> None:
    _sync_streamlit_secrets_to_env()
    logger = _get_logger()
    _init_state()

    st.title("📊 Wildberries — Аналитика")
    _render_theme_css()

    tab_ads, tab_positions = st.tabs(["Рекламная статистика", "Позиции в поиске"])

    with tab_ads:
        _render_ads_tab(logger)

    with tab_positions:
        _render_positions_tab(logger)


if __name__ == "__main__":
    main()

