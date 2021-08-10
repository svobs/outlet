import time
from datetime import datetime, timezone


def now_ms():
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def now_sec():
    """Currently, sync timestamps will be stored in seconds resolution"""
    return int(time.time())
