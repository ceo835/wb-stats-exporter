"""Data processing, metrics, charts and Excel export."""

from __future__ import annotations

from datetime import date as date_type
from io import BytesIO
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

NUMERIC_COLUMNS = [
    "spend",
    "revenue",
    "views",
    "clicks",
    "atbs",
    "orders",
    "ordered_items",
    "canceled",
    "avg_position",
]

RAW_DISPLAY_COLUMNS = [
    "date",
    "campaign_id",
    "campaign_name",
    "row_type",
    "conversion_type",
    "nm_id",
    "nm_name",
    "spend",
    "revenue",
    "views",
    "clicks",
    "atbs",
    "orders",
    "ordered_items",
    "canceled",
    "avg_position",
    "CTR",
    "CPC",
    "CPM",
    "CR",
    "ROI",
]

RAW_RENAME_MAP = {
    "date": "Дата",
    "campaign_id": "ID кампании",
    "campaign_name": "Название кампании",
    "currency": "Валюта",
    "row_type": "Тип строки",
    "app_type": "Тип атрибуции",
    "conversion_type": "Тип конверсии",
    "nm_id": "Номенклатура",
    "nm_name": "Название товара",
    "spend": "Затраты, ₽",
    "revenue": "Выручка, ₽",
    "views": "Показы",
    "clicks": "Клики",
    "atbs": "Добавления в корзину",
    "orders": "Заказы",
    "ordered_items": "Заказанные товары, шт.",
    "canceled": "Отмены",
    "avg_position": "Средняя позиция",
    "CTR": "CTR, %",
    "CPC": "CPC, ₽",
    "CPM": "CPM, ₽",
    "CR": "CR, %",
    "ROI": "ROI, %",
}

SUMMARY_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "spend",
    "revenue",
    "views",
    "clicks",
    "atbs",
    "orders",
    "ordered_items",
    "canceled",
    "CTR",
    "CPC",
    "CPM",
    "CR",
    "ROI",
]

SUMMARY_RENAME_MAP = {
    "campaign_id": "ID кампании",
    "campaign_name": "Название кампании",
    "spend": "Потрачено, ₽",
    "revenue": "Выручка, ₽",
    "views": "Показы",
    "clicks": "Клики",
    "atbs": "Добавления в корзину",
    "orders": "Заказы",
    "ordered_items": "Заказанные товары, шт.",
    "canceled": "Отмены",
    "CTR": "CTR, %",
    "CPC": "CPC, ₽",
    "CPM": "CPM, ₽",
    "CR": "CR, %",
    "ROI": "ROI, %",
}

REPORT_SHEET_ORDER = [
    "Сводные",
    "Кампании",
    "Товары",
    "Товары детально",
]

METRIC_COLUMNS_FOR_VALIDATION = ["spend", "clicks", "orders", "views", "revenue"]
ROI_MIN_SPEND = 1.0
ROW_TYPE_DISPLAY_MAP = {
    "campaign_total": "Итог кампании",
    "item": "Товар",
}


def prepare_data(
    rows: list[dict[str, Any]],
    filter_zero_spend: bool = False,
    table_mode: str = "items",
    aggregate_items: bool = True,
) -> dict[str, Any]:
    """Prepare datasets for UI and validations.

    table_mode:
        - "totals": only campaign_total rows
        - "items": only item rows
        - "all": all rows
    aggregate_items:
        - True: merge equal products in table by campaign_id + nm_id
        - False: keep detailed item rows
    filter_zero_spend:
        - True: hide only zero-spend item rows; keep campaign_total rows
    """
    if not rows:
        empty = pd.DataFrame()
        return {
            "raw_df": empty,
            "table_df": empty,
            "campaign_daily_df": empty,
            "summary_df": empty,
            "metrics": {"spent": 0.0, "clicks": 0, "orders": 0, "ctr": 0.0},
            "warnings": [],
        }

    df = pd.DataFrame(rows).copy()
    df = _normalize_dataframe(df)
    df = _filter_noise_rows(df)

    warnings: list[str] = []
    warnings.extend(_validate_future_dates(df))
    warnings.extend(_validate_item_totals_consistency(df))

    if filter_zero_spend:
        df = _filter_zero_spend_item_rows(df)

    if df.empty:
        empty = pd.DataFrame()
        return {
            "raw_df": empty,
            "table_df": empty,
            "campaign_daily_df": empty,
            "summary_df": empty,
            "metrics": {"spent": 0.0, "clicks": 0, "orders": 0, "ctr": 0.0},
            "warnings": warnings,
        }

    campaign_daily_df = _build_campaign_daily(df)
    summary_df = _build_campaign_summary(campaign_daily_df)
    table_df = _build_table_df(df, mode=table_mode, aggregate_items=aggregate_items)
    metrics = _build_overall_metrics(campaign_daily_df)

    return {
        "raw_df": df,
        "table_df": table_df,
        "campaign_daily_df": campaign_daily_df,
        "summary_df": summary_df,
        "metrics": metrics,
        "warnings": warnings,
    }


def _filter_noise_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop service/noise item rows (e.g., `Тип 0`, empty nm_id)."""
    if df.empty:
        return df

    filtered = df.copy()

    if "conversion_type" not in filtered.columns:
        filtered["conversion_type"] = ""
    if "row_type" not in filtered.columns:
        filtered["row_type"] = ""
    if "nm_id" not in filtered.columns:
        filtered["nm_id"] = pd.NA
    if "app_type" not in filtered.columns:
        filtered["app_type"] = pd.NA

    # 1) Remove service rows marked as "Тип 0" (or app_type=0 fallback)
    conversion_type_mask = (
        filtered["conversion_type"]
        .astype(str)
        .str.contains(r"\bТип\s*0\b", regex=True, case=False, na=False)
    )
    app_type_mask = pd.to_numeric(filtered["app_type"], errors="coerce").eq(0)
    service_type0_mask = conversion_type_mask | app_type_mask
    filtered = filtered[~service_type0_mask].copy()

    # 2) Keep only item rows with nm_id filled; campaign_total rows remain untouched
    nm_numeric = pd.to_numeric(filtered["nm_id"], errors="coerce")
    is_item = filtered["row_type"].astype(str).eq("item")
    keep_mask = (~is_item) | nm_numeric.notna()
    filtered = filtered[keep_mask].copy()

    return filtered.reset_index(drop=True)


def _filter_zero_spend_item_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop only item rows with zero spend; keep campaign totals intact."""
    if df.empty:
        return df

    filtered = df.copy()
    if "row_type" not in filtered.columns:
        filtered["row_type"] = ""
    if "spend" not in filtered.columns:
        filtered["spend"] = 0

    is_item = filtered["row_type"].astype(str).eq("item")
    positive_spend = pd.to_numeric(filtered["spend"], errors="coerce").fillna(0).gt(0)
    keep_mask = (~is_item) | positive_spend
    return filtered[keep_mask].reset_index(drop=True)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert types, fill NaN and compute derived metrics."""
    for column in NUMERIC_COLUMNS:
        if column not in df.columns:
            df[column] = 0
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    for column in ("campaign_name", "conversion_type", "nm_name", "row_type"):
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].fillna("").astype(str)

    if "campaign_id" in df.columns:
        df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors="coerce").fillna(0).astype(int)
    else:
        df["campaign_id"] = 0

    if "date" not in df.columns:
        df["date"] = ""
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d").fillna("")

    df = _add_derived_metrics(df)
    df = df.sort_values(by=["date", "campaign_id"], ascending=[False, True]).reset_index(drop=True)
    return df


def _safe_ratio(
    numerator: pd.Series,
    denominator: pd.Series,
    *,
    multiplier: float = 1.0,
    min_denominator: float = 0.0,
) -> pd.Series:
    """Safe ratio with None for invalid denominator."""
    denominator = pd.to_numeric(denominator, errors="coerce")
    numerator = pd.to_numeric(numerator, errors="coerce")
    valid = denominator > min_denominator

    result = pd.Series([pd.NA] * len(denominator), index=denominator.index, dtype="object")
    result.loc[valid] = (numerator.loc[valid] / denominator.loc[valid]) * multiplier
    return pd.to_numeric(result, errors="coerce")


def _safe_roi(revenue: pd.Series, spend: pd.Series) -> pd.Series:
    """ROI with protection from zero and micro-spend explosion."""
    spend_num = pd.to_numeric(spend, errors="coerce")
    revenue_num = pd.to_numeric(revenue, errors="coerce")

    valid = spend_num >= ROI_MIN_SPEND
    result = pd.Series([pd.NA] * len(spend_num), index=spend_num.index, dtype="object")
    result.loc[valid] = ((revenue_num.loc[valid] - spend_num.loc[valid]) / spend_num.loc[valid]) * 100.0
    return pd.to_numeric(result, errors="coerce")


def _add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add CTR, CPC, CPM, CR, ROI with safe division rules."""
    df["CTR"] = _safe_ratio(df["clicks"], df["views"], multiplier=100.0).round(2)

    cpc = _safe_ratio(df["spend"], df["clicks"])
    cpc = cpc.where((df["clicks"] > 0) & (df["spend"] > 0), pd.NA)
    df["CPC"] = cpc.round(2)

    cpm = _safe_ratio(df["spend"], df["views"], multiplier=1000.0)
    cpm = cpm.where((df["views"] > 0) & (df["spend"] > 0), pd.NA)
    df["CPM"] = cpm.round(2)

    df["CR"] = _safe_ratio(df["orders"], df["clicks"], multiplier=100.0).round(2)
    df["ROI"] = _safe_roi(df["revenue"], df["spend"]).round(2)
    return df


def _build_campaign_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Build one daily row per campaign for charts/metrics."""
    total_rows = df[df["row_type"] == "campaign_total"].copy()
    if not total_rows.empty:
        return _normalize_dataframe(total_rows)

    grouped = (
        df.groupby(["date", "campaign_id", "campaign_name"], as_index=False)[
            ["spend", "revenue", "views", "clicks", "atbs", "orders", "ordered_items", "canceled"]
        ]
        .sum()
        .copy()
    )
    grouped["row_type"] = "campaign_total"
    grouped["conversion_type"] = ""
    grouped["nm_id"] = 0
    grouped["nm_name"] = ""
    grouped["avg_position"] = 0.0
    grouped["app_type"] = 0
    grouped["currency"] = "RUB"
    return _normalize_dataframe(grouped)


def _build_campaign_summary(campaign_daily_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate metrics by campaign_id."""
    if campaign_daily_df.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    summary = (
        campaign_daily_df.groupby(["campaign_id", "campaign_name"], as_index=False)[
            ["spend", "revenue", "views", "clicks", "atbs", "orders", "ordered_items", "canceled"]
        ]
        .sum()
        .copy()
    )
    summary["CTR"] = _safe_ratio(summary["clicks"], summary["views"], multiplier=100.0).round(2)
    summary["CPC"] = _safe_ratio(summary["spend"], summary["clicks"]).round(2)
    summary["CPM"] = _safe_ratio(summary["spend"], summary["views"], multiplier=1000.0).round(2)
    summary["CR"] = _safe_ratio(summary["orders"], summary["clicks"], multiplier=100.0).round(2)
    summary["ROI"] = _safe_roi(summary["revenue"], summary["spend"]).round(2)

    summary = summary[SUMMARY_COLUMNS].sort_values(by="spend", ascending=False).reset_index(drop=True)
    return summary


def _build_table_df(
    df: pd.DataFrame,
    mode: str = "items",
    aggregate_items: bool = True,
) -> pd.DataFrame:
    """Create table dataframe for UI."""
    if mode == "totals":
        result = df[df["row_type"] == "campaign_total"].copy()
    elif mode == "all":
        result = df.copy()
    else:
        result = df[df["row_type"] == "item"].copy()
        if result.empty:
            result = df.copy()

    if aggregate_items:
        if mode == "items":
            result = _aggregate_items_for_table(result)
        elif mode == "all":
            totals = result[result["row_type"] == "campaign_total"].copy()
            items = _aggregate_items_for_table(result)
            result = pd.concat([totals, items], ignore_index=True)
            result = result.sort_values(by=["campaign_id", "row_type"], ascending=[True, False]).reset_index(drop=True)

    return _to_display_table(result)


def _to_display_table(result: pd.DataFrame) -> pd.DataFrame:
    """Convert internal dataframe to UI/export display columns."""
    columns = [column for column in RAW_DISPLAY_COLUMNS if column in result.columns]
    result = result[columns].rename(columns=RAW_RENAME_MAP)
    if "Тип строки" in result.columns:
        result["Тип строки"] = result["Тип строки"].replace(ROW_TYPE_DISPLAY_MAP)

    nan_placeholder_columns = [
        RAW_RENAME_MAP[key]
        for key in ("CTR", "CPC", "CPM", "CR", "ROI", "avg_position")
        if key in RAW_RENAME_MAP
    ]
    for metric_column in nan_placeholder_columns:
        if metric_column in result.columns:
            result[metric_column] = result[metric_column].where(result[metric_column].notna(), "—")

    return result.reset_index(drop=True)


def _aggregate_items_for_table(item_df: pd.DataFrame) -> pd.DataFrame:
    """Merge equal products by campaign_id + nm_id across selected period."""
    if item_df.empty:
        return item_df

    base_items = item_df[item_df["row_type"] == "item"].copy()
    if base_items.empty:
        return item_df

    base_items["nm_id"] = pd.to_numeric(base_items["nm_id"], errors="coerce").astype("Int64")

    grouped = (
        base_items.groupby(["campaign_id", "campaign_name", "nm_id"], as_index=False, dropna=False)
        .agg(
            nm_name=("nm_name", _first_non_empty),
            spend=("spend", "sum"),
            revenue=("revenue", "sum"),
            views=("views", "sum"),
            clicks=("clicks", "sum"),
            atbs=("atbs", "sum"),
            orders=("orders", "sum"),
            ordered_items=("ordered_items", "sum"),
            canceled=("canceled", "sum"),
        )
        .copy()
    )

    grouped["nm_id"] = grouped["nm_id"].fillna(0).astype(int)
    grouped["date"] = "За период"
    grouped["row_type"] = "item"
    grouped["conversion_type"] = "Суммарно"
    grouped["avg_position"] = pd.NA
    grouped["app_type"] = pd.NA
    grouped["currency"] = "RUB"

    grouped = _add_derived_metrics(grouped)
    grouped = grouped.sort_values(by=["campaign_id", "ordered_items"], ascending=[True, False]).reset_index(drop=True)
    return grouped


def _aggregate_items_detailed_for_table(item_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate items by campaign + nm_id + conversion_type (keep attribution split)."""
    if item_df.empty:
        return item_df

    base_items = item_df[item_df["row_type"] == "item"].copy()
    if base_items.empty:
        return item_df

    base_items["nm_id"] = pd.to_numeric(base_items["nm_id"], errors="coerce").astype("Int64")
    base_items["conversion_type"] = base_items["conversion_type"].fillna("").astype(str)

    grouped = (
        base_items.groupby(
            ["campaign_id", "campaign_name", "nm_id", "conversion_type"],
            as_index=False,
            dropna=False,
        )
        .agg(
            nm_name=("nm_name", _first_non_empty),
            app_type=("app_type", "first"),
            spend=("spend", "sum"),
            revenue=("revenue", "sum"),
            views=("views", "sum"),
            clicks=("clicks", "sum"),
            atbs=("atbs", "sum"),
            orders=("orders", "sum"),
            ordered_items=("ordered_items", "sum"),
            canceled=("canceled", "sum"),
        )
        .copy()
    )

    grouped["nm_id"] = grouped["nm_id"].fillna(0).astype(int)
    grouped["date"] = "За период"
    grouped["row_type"] = "item"
    grouped["avg_position"] = pd.NA
    grouped["currency"] = "RUB"

    grouped = _add_derived_metrics(grouped)
    grouped = grouped.sort_values(
        by=["campaign_id", "nm_id", "conversion_type"],
        ascending=[True, True, True],
    ).reset_index(drop=True)
    return grouped


def _prepare_summary_export(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare summary sheet columns and labels."""
    summary_export = summary_df.copy()
    if summary_export.empty:
        summary_export = pd.DataFrame(columns=SUMMARY_COLUMNS)
    summary_export = summary_export[[column for column in SUMMARY_COLUMNS if column in summary_export.columns]]
    summary_export = summary_export.rename(columns=SUMMARY_RENAME_MAP)
    return summary_export


def build_report_sheets(raw_df: pd.DataFrame, summary_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build final report tables used both in UI and Excel export."""
    base_raw = raw_df.copy()
    if base_raw.empty:
        base_raw = pd.DataFrame(columns=RAW_DISPLAY_COLUMNS)

    campaign_rows = base_raw[base_raw["row_type"] == "campaign_total"].copy()
    if campaign_rows.empty:
        campaign_rows = pd.DataFrame(columns=RAW_DISPLAY_COLUMNS)

    item_rows = base_raw[base_raw["row_type"] == "item"].copy()

    items_aggregated_rows = _aggregate_items_for_table(item_rows)
    if items_aggregated_rows.empty:
        items_aggregated_rows = pd.DataFrame(columns=RAW_DISPLAY_COLUMNS)

    items_detailed_rows = _aggregate_items_detailed_for_table(item_rows)
    if items_detailed_rows.empty:
        items_detailed_rows = pd.DataFrame(columns=RAW_DISPLAY_COLUMNS)

    return {
        "Сводные": _prepare_summary_export(summary_df),
        "Кампании": _to_display_table(campaign_rows),
        "Товары": _to_display_table(items_aggregated_rows),
        "Товары детально": _to_display_table(items_detailed_rows),
    }


def _first_non_empty(series: pd.Series) -> str:
    """Return first non-empty string from a series."""
    for value in series:
        text = str(value).strip()
        if text:
            return text
    return ""


def _build_overall_metrics(campaign_daily_df: pd.DataFrame) -> dict[str, Any]:
    """Compute KPI cards from campaign totals only."""
    if campaign_daily_df.empty:
        return {"spent": 0.0, "clicks": 0, "orders": 0, "ctr": 0.0}

    base = campaign_daily_df[campaign_daily_df["row_type"] == "campaign_total"].copy()
    if base.empty:
        base = campaign_daily_df

    spent = float(base["spend"].sum())
    clicks = int(base["clicks"].sum())
    orders = int(base["orders"].sum())
    views = int(base["views"].sum())
    ctr = round((clicks / views * 100) if views else 0.0, 2)

    return {"spent": round(spent, 2), "clicks": clicks, "orders": orders, "ctr": ctr}


def _validate_future_dates(df: pd.DataFrame) -> list[str]:
    """Check that report does not contain future dates."""
    warnings: list[str] = []
    if df.empty or "date" not in df.columns:
        return warnings

    current = date_type.today().isoformat()
    future_dates = sorted(date_str for date_str in df["date"].dropna().astype(str).unique() if date_str > current)
    if future_dates:
        warnings.append(
            "Обнаружены даты в будущем: "
            + ", ".join(future_dates[:5])
            + ("..." if len(future_dates) > 5 else "")
        )
    return warnings


def _validate_item_totals_consistency(df: pd.DataFrame, tolerance: float = 0.01) -> list[str]:
    """Validate that item sums equal campaign totals per campaign/date."""
    warnings: list[str] = []
    if df.empty:
        return warnings

    totals = df[df["row_type"] == "campaign_total"].copy()
    items = df[df["row_type"] == "item"].copy()
    if totals.empty or items.empty:
        return warnings

    grouped_totals = (
        totals.groupby(["date", "campaign_id"], as_index=False)[METRIC_COLUMNS_FOR_VALIDATION]
        .sum()
        .set_index(["date", "campaign_id"])
    )
    grouped_items = (
        items.groupby(["date", "campaign_id"], as_index=False)[METRIC_COLUMNS_FOR_VALIDATION]
        .sum()
        .set_index(["date", "campaign_id"])
    )

    common_index = grouped_totals.index.intersection(grouped_items.index)
    mismatches: list[str] = []

    for key in common_index:
        total_row = grouped_totals.loc[key]
        item_row = grouped_items.loc[key]
        campaign_date, campaign_id = key

        for metric in METRIC_COLUMNS_FOR_VALIDATION:
            total_value = float(total_row[metric])
            item_value = float(item_row[metric])
            if abs(total_value - item_value) > tolerance:
                mismatches.append(
                    f"campaign_id={campaign_id}, date={campaign_date}, metric={metric}, "
                    f"campaign_total={total_value:.2f}, items_sum={item_value:.2f}"
                )
                break

    if mismatches:
        warnings.append(
            f"Найдены расхождения item vs campaign_total: {len(mismatches)}. "
            f"Пример: {mismatches[0]}"
        )
    return warnings


def build_spend_trend_chart(campaign_daily_df: pd.DataFrame, dark_mode: bool = False) -> go.Figure:
    """Line chart: date x spend (grouped by campaign)."""
    if campaign_daily_df.empty:
        fig = go.Figure()
        fig.update_layout(title="Динамика расходов")
        return fig

    trend_df = campaign_daily_df[campaign_daily_df["row_type"] == "campaign_total"].copy()
    if trend_df.empty:
        trend_df = campaign_daily_df.copy()

    trend_df["campaign_title"] = trend_df["campaign_name"].where(
        trend_df["campaign_name"].str.strip() != "",
        trend_df["campaign_id"].astype(str),
    )

    fig = px.line(
        trend_df,
        x="date",
        y="spend",
        color="campaign_title",
        markers=True,
        labels={
            "date": "Дата",
            "spend": "Расход, ₽",
            "campaign_title": "Кампания",
        },
        title="Динамика расходов по кампаниям",
    )
    fig.update_layout(template="plotly_dark" if dark_mode else "plotly_white", height=420)
    return fig


def build_top_campaigns_chart(summary_df: pd.DataFrame, dark_mode: bool = False) -> go.Figure:
    """Bar chart: top-5 campaigns by orders."""
    if summary_df.empty:
        fig = go.Figure()
        fig.update_layout(title="Топ-5 кампаний по заказам")
        return fig

    top_df = summary_df.nlargest(5, "orders").copy()
    top_df["campaign_title"] = top_df["campaign_name"].where(
        top_df["campaign_name"].str.strip() != "",
        top_df["campaign_id"].astype(str),
    )

    fig = px.bar(
        top_df,
        x="campaign_title",
        y="orders",
        text="orders",
        labels={"campaign_title": "Кампания", "orders": "Заказы"},
        title="Топ-5 кампаний по заказам",
    )
    fig.update_layout(template="plotly_dark" if dark_mode else "plotly_white", height=420)
    fig.update_traces(textposition="outside")
    return fig


def build_funnel_chart(campaign_daily_df: pd.DataFrame, dark_mode: bool = False) -> go.Figure:
    """Funnel chart: views -> clicks -> orders."""
    base = campaign_daily_df[campaign_daily_df["row_type"] == "campaign_total"].copy()
    if base.empty:
        base = campaign_daily_df

    views = int(base["views"].sum()) if not base.empty else 0
    clicks = int(base["clicks"].sum()) if not base.empty else 0
    orders = int(base["orders"].sum()) if not base.empty else 0

    fig = go.Figure(
        go.Funnel(
            y=["Показы", "Клики", "Заказы"],
            x=[views, clicks, orders],
            textinfo="value+percent initial",
        )
    )
    fig.update_layout(
        title="Воронка",
        template="plotly_dark" if dark_mode else "plotly_white",
        height=420,
    )
    return fig


def build_excel_report(
    raw_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> tuple[bytes, str]:
    """Build xlsx report with formatting and report sheets."""
    report_sheets = build_report_sheets(raw_df=raw_df, summary_df=summary_df)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name in REPORT_SHEET_ORDER:
            table = report_sheets.get(sheet_name, pd.DataFrame())
            table.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        for worksheet_name in REPORT_SHEET_ORDER:
            worksheet = workbook[worksheet_name]
            _style_worksheet(worksheet)

    output.seek(0)
    file_name = f"WB_Report_{start_date}_{end_date}.xlsx"
    return output.read(), file_name


def _style_worksheet(worksheet: Any) -> None:
    """Apply header style and auto-width for worksheet."""
    header_font = Font(bold=True)
    for cell in worksheet[1]:
        cell.font = header_font

    for col_idx, column_cells in enumerate(worksheet.columns, start=1):
        max_length = 0
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        worksheet.column_dimensions[get_column_letter(col_idx)].width = min(max(max_length + 2, 10), 45)
