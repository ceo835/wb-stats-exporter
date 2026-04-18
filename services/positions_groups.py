"""Grouping helpers for positions reporting."""

from __future__ import annotations

from typing import Any

POSITION_CATEGORY_WOMEN_PANTIES = "Женские трусы (все)"
POSITION_CATEGORY_MEN = "Мужские"
POSITION_CATEGORY_WOMEN_TSHIRTS = "Футболки женские"
POSITION_CATEGORY_MEN_TSHIRTS = "Футболки мужские"
POSITION_CATEGORY_GIRLS_PANTIES = "Трусы для девочек"
POSITION_CATEGORY_BOYS_PANTIES = "Трусы для мальчиков"
POSITION_CATEGORY_OTHER = "Прочее"

POSITION_CATEGORY_ORDER = [
    POSITION_CATEGORY_WOMEN_PANTIES,
    POSITION_CATEGORY_MEN,
    POSITION_CATEGORY_WOMEN_TSHIRTS,
    POSITION_CATEGORY_MEN_TSHIRTS,
    POSITION_CATEGORY_GIRLS_PANTIES,
    POSITION_CATEGORY_BOYS_PANTIES,
]


def classify_position_category(product_name: Any, user_query: Any, matched_query: Any = "") -> str:
    """Classify row into one of predefined user-facing categories."""
    text = _normalize_text(" ".join(str(value or "").strip() for value in (product_name, user_query, matched_query)))

    is_tshirt = "футбол" in text
    is_underwear = _contains_any(text, ("трус", "трусик", "трусики", "слип", "боксер", "боксер"))

    has_girl = _contains_any(text, ("девоч", "девч"))
    has_boy = _contains_any(text, ("мальч",))
    has_female = _contains_any(text, ("жен", "женск", "женщ", "девуш", "девоч", "девч"))
    has_male = _contains_any(text, ("муж", "мужск", "мужчин", "парн", "мальч"))

    if is_underwear and has_girl:
        return POSITION_CATEGORY_GIRLS_PANTIES
    if is_underwear and has_boy:
        return POSITION_CATEGORY_BOYS_PANTIES

    if is_tshirt and has_girl:
        return POSITION_CATEGORY_WOMEN_TSHIRTS
    if is_tshirt and has_boy:
        return POSITION_CATEGORY_MEN_TSHIRTS
    if is_tshirt and has_female and not has_male:
        return POSITION_CATEGORY_WOMEN_TSHIRTS
    if is_tshirt and has_male and not has_female:
        return POSITION_CATEGORY_MEN_TSHIRTS

    if is_underwear and has_male and not has_female:
        return POSITION_CATEGORY_MEN
    if is_underwear:
        return POSITION_CATEGORY_WOMEN_PANTIES

    return POSITION_CATEGORY_OTHER


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("\u0451", "\u0435")
    return " ".join(text.split())

