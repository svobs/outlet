import logging

from constants import TreeType
from model.uid import UID

logger = logging.getLogger(__name__)


def ensure_int(val):
    try:
        if type(val) == str:
            return int(val)
    except ValueError:
        logger.error(f'Bad value: {val}')
    return val


def ensure_uid(val):
    try:
        if val and not isinstance(val, UID):
            return UID(ensure_int(val))
    except ValueError:
        logger.error(f'Bad value: {val}')
    return val


def ensure_bool(val):
    try:
        return bool(val)
    except ValueError:
        pass
    return val


def ensure_list(full_path):
    if full_path:
        if type(full_path) == list:
            return full_path
        else:
            return [full_path]
    else:
        return []


def ensure_tree_type(val):
    try:
        if val and not isinstance(val, TreeType):
            return TreeType(ensure_int(val))
    except ValueError:
        logger.error(f'Bad value: {val}')
    return val
