import time
from datetime import datetime, timezone

import pytz


def now_ms():
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def now_sec():
    """Currently, sync timestamps will be stored in seconds resolution"""
    return int(time.time())


def ts_to_rfc_3339(ts: int) -> str:
    """Assumes we are taking time in ms as arg"""
    dt = datetime.fromtimestamp(ts / 1000)
    dt_with_timezone = dt.replace(tzinfo=pytz.UTC)
    return dt_with_timezone.isoformat()
