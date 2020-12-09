import time
from datetime import datetime


def now_ms():
    return int(datetime.now().microsecond / 1000)


def now_sec():
    """Currently, sync timestamps will be stored in seconds resolution"""
    return int(time.time())
