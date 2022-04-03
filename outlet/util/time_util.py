import time
from datetime import datetime, timezone

import pytz

from constants import TS_FORMAT_WITH_MILLIS


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


def ts_to_str(ts: int, fmt: str = '%Y-%m-%d %H:%M:%S') -> str:
    """Note: this cannot print milliseconds. For that, use ts_to_str_with_millis()"""
    dt = datetime.fromtimestamp(ts / 1000)
    return dt.strftime(fmt)


def ts_to_str_with_millis(ts: int) -> str:
    """
    Prints the given timestamp to millisecond precision.
    See: https://stackoverflow.com/a/35643540/1347529
    """
    dt = datetime.fromtimestamp(ts / 1000)
    [dt, microsec] = dt.strftime(TS_FORMAT_WITH_MILLIS).split('.')
    return '%s%03d' % (dt, int(microsec) / 1000)
