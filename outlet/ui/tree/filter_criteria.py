from enum import IntEnum
from typing import List
import logging
from constants import SUPER_DEBUG, TrashStatus, TREE_TYPE_GDRIVE
from model.node.node import Node

logger = logging.getLogger(__name__)


#    CLASS BoolOption
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BoolOption(IntEnum):
    """Allows for 3 values for boolean option: in addition to the standard True/False values, also allows Unspecified"""
    FALSE = 0
    TRUE = 1
    NOT_SPECIFIED = 2


#    CLASS FilterCriteria
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class FilterCriteria:
    def __init__(self, search_query: str = None, is_trashed: BoolOption = BoolOption.NOT_SPECIFIED, is_shared: BoolOption = BoolOption.NOT_SPECIFIED):
        self.search_query: str = search_query
        self.ignore_case: bool = False
        self.is_trashed: BoolOption = is_trashed
        self.is_shared: BoolOption = is_shared

    def has_criteria(self) -> bool:
        return self.search_query or self.is_trashed != BoolOption.NOT_SPECIFIED or self.is_shared != BoolOption.NOT_SPECIFIED

    def matches_search_query(self, node) -> bool:
        return not self.search_query or (self.ignore_case and self.search_query.lower() in node.name.lower()) or self.search_query in node.name

    def matches_trashed(self, node) -> bool:
        return self.is_trashed == BoolOption.NOT_SPECIFIED or (self.is_trashed == BoolOption.FALSE and node.trashed() == TrashStatus.NOT_TRASHED) or \
                (self.is_trashed == BoolOption.TRUE and (node.trashed() == TrashStatus.EXPLICITLY_TRASHED
                                                         or node.trashed() == TrashStatus.IMPLICITLY_TRASHED))

    def matches_is_shared(self, node) -> bool:
        return self.is_shared == BoolOption.NOT_SPECIFIED or \
               (node.get_tree_type() == TREE_TYPE_GDRIVE and node.is_shared() == bool(self.is_shared))

    def filter(self, node_list: List[Node]) -> List[Node]:
        filtered_list: List[Node] = []

        for node in node_list:
            if not self.matches_search_query(node):
                if SUPER_DEBUG:
                    logger.debug(f'Search query "{self.search_query}" not found in "{node.name}" (ignore_case={self.ignore_case})')
                continue

            if not self.matches_trashed(node):
                if SUPER_DEBUG:
                    logger.debug(f'Node with TrashStatus={node.trashed()} does not match trashed={self.is_trashed.name}')
                continue

            if not self.matches_is_shared(node):
                if SUPER_DEBUG:
                    logger.debug(f'Node with IsShared={node.is_shared()} does not match shared={self.is_shared.name}')
                continue

            filtered_list.append(node)

        return filtered_list
