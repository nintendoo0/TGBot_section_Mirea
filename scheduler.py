from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def moscow_now() -> datetime:
    return datetime.now(tz=MOSCOW_TZ)


def parse_publish_datetime_msk(text: str) -> datetime | None:
    """Parse 'ДД.ММ.ГГГГ ЧЧ:ММ' in Europe/Moscow timezone."""
    raw = " ".join((text or "").strip().split())
    try:
        dt = datetime.strptime(raw, "%d.%m.%Y %H:%M")
    except ValueError:
        return None

    return dt.replace(tzinfo=MOSCOW_TZ)


def to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")
