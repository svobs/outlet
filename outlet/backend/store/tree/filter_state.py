from typing import Deque, Dict, List
import logging

from collections import deque
from constants import TrashStatus, TREE_TYPE_GDRIVE
from model.has_get_children import HasGetChildren
from model.display_tree.filter_criteria import FilterCriteria, Ternary
from model.node.node import Node
from model.uid import UID
from util.ensure import ensure_bool

logger = logging.getLogger(__name__)


class FilterState:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS FilterState

    Internal to the backend: this keeps track of the current FilterState for a given tree, and handles matching and
    match caching logic.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, filter_criteria: FilterCriteria):
        self.filter: FilterCriteria = filter_criteria

        self._cached_filter: Dict[str, Dict[UID, List[Node]]] = {}

    def _matches_search_query(self, node) -> bool:
        return not self.filter.search_query or (self.filter.ignore_case and self.filter.search_query.lower() in node.name.lower()) \
               or self.filter.search_query in node.name

    def _matches_trashed(self, node) -> bool:
        return self.filter.is_trashed == Ternary.NOT_SPECIFIED or \
               (self.filter.is_trashed == Ternary.FALSE and node.get_trashed_status() == TrashStatus.NOT_TRASHED) or \
               (self.filter.is_trashed == Ternary.TRUE and node.get_trashed_status() != TrashStatus.NOT_TRASHED)

    def _matches_is_shared(self, node) -> bool:
        return self.filter.is_shared == Ternary.NOT_SPECIFIED or node.get_tree_type() != TREE_TYPE_GDRIVE \
               or node.is_shared == bool(self.filter.is_shared)

    def matches(self, node) -> bool:
        if not self._matches_search_query(node):
            # if SUPER_DEBUG:
            #     logger.debug(f'Search query "{self.search_query}" not found in "{node.name}" (ignore_case={self.ignore_case})')
            return False

        if not self._matches_trashed(node):
            # if SUPER_DEBUG:
            #     logger.debug(f'Node with TrashStatus={node.get_trashed_status()} does not match trashed={self.is_trashed}')
            return False

        if not self._matches_is_shared(node):
            # if SUPER_DEBUG:
            #     logger.debug(f'Node with IsShared={node.is_shared()} does not match shared={self.is_shared}')
            return False

        return True

    def _build_node_dict(self, parent_tree: HasGetChildren, subtree_root_node: Node):
        logger.debug(f'Building filtered node dict for subroot {subtree_root_node.node_identifier}')
        node_dict: Dict[UID, List[Node]] = {}

        dir_queue: Deque[Node] = deque()
        second_pass_stack: Deque[Node] = deque()

        if subtree_root_node.is_dir():
            dir_queue.append(subtree_root_node)
            second_pass_stack.append(subtree_root_node)

        # First pass: find all directories in BFS order and append to second_pass_stack
        while len(dir_queue) > 0:
            node: Node = dir_queue.popleft()

            children = parent_tree.get_children(node)
            if children:
                for child in children:
                    if child.is_dir():
                        dir_queue.append(child)
                        second_pass_stack.append(child)

        while len(second_pass_stack) > 0:
            parent_node = second_pass_stack.pop()
            child_list = parent_tree.get_children(parent_node)
            filtered_child_list = []
            for child in child_list:
                # include dirs if any of their children are included, or if they match. Include non-dirs only if they match:
                if (child.is_dir() and child.uid in node_dict) or self.matches(child):
                    filtered_child_list.append(child)
            if filtered_child_list:
                node_dict[parent_node.uid] = filtered_child_list

        logger.debug(f'Built filtered node dict with {len(node_dict)} entries')
        return node_dict

    def _hash_current_filter(self) -> str:
        return f'{int(self.filter.show_subtrees_of_matches)}:{int(self.filter.ignore_case)}:{int(self.filter.is_trashed)}:' \
               f'{int(self.filter.is_shared)}:{self.filter.search_query}'

    def get_filtered_child_list(self, parent_node: Node, parent_tree: HasGetChildren) -> List[Node]:
        if not self.filter.has_criteria():
            # logger.debug(f'No FilterCriteria selected; returning unfiltered list')
            return parent_tree.get_children(parent_node)

        filtered_list: List[Node] = []

        if self.filter.show_subtrees_of_matches:
            # TODO: it's pretty rickety to assume the first call will be the topmost level. Put cache in a better spot
            hash_val = self._hash_current_filter()
            cached_tree = self._cached_filter.get(hash_val, None)
            if not cached_tree:
                cached_tree = self._build_node_dict(parent_tree, parent_node)
                self._cached_filter[hash_val] = cached_tree

            return cached_tree.get(parent_node.uid, [])
        else:
            queue: Deque[Node] = deque()
            for node in parent_tree.get_children(parent_node):
                queue.append(node)

            while len(queue) > 0:
                node: Node = queue.popleft()

                if self.matches(node):
                    filtered_list.append(node)

                if node.is_dir():
                    for child_node in parent_tree.get_children(node):
                        queue.append(child_node)

        return filtered_list

    @staticmethod
    def _make_search_query_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.filter.search_query'

    @staticmethod
    def _make_ignore_case_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.filter.ignore_case'

    @staticmethod
    def _make_is_trashed_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.filter.is_trashed'

    @staticmethod
    def _make_is_shared_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.filter.is_shared'

    @staticmethod
    def _make_show_subtree_config_key(tree_id: str) -> str:
        return f'ui_state.{tree_id}.filter.show_subtrees'

    def write_to_config(self, backend, tree_id: str):
        search_query = self.filter.search_query
        if not search_query:
            search_query = ''
        logger.debug(f'[{tree_id}] Writing FilterCriteria to config with search query: "{search_query}"')
        backend.put_config(FilterState._make_search_query_config_key(tree_id), search_query)

        backend.put_config(FilterState._make_ignore_case_config_key(tree_id), self.filter.ignore_case)

        backend.put_config(FilterState._make_is_trashed_config_key(tree_id), self.filter.is_trashed)

        backend.put_config(FilterState._make_is_shared_config_key(tree_id), self.filter.is_shared)

        backend.put_config(FilterState._make_show_subtree_config_key(tree_id), self.filter.show_subtrees_of_matches)

    @staticmethod
    def from_config(backend, tree_id: str):
        assert tree_id, 'No tree_id specified!'
        logger.debug(f'[{tree_id}] Reading FilterCriteria from config')
        filter_criteria = FilterCriteria()

        search_query = backend.get_config(FilterState._make_search_query_config_key(tree_id), '')
        filter_criteria.search_query = search_query

        ignore_case = ensure_bool(backend.get_config(FilterState._make_ignore_case_config_key(tree_id), False))
        filter_criteria.ignore_case = ignore_case

        is_trashed = backend.get_config(FilterState._make_is_trashed_config_key(tree_id), Ternary.NOT_SPECIFIED)
        is_trashed = Ternary(is_trashed)
        filter_criteria.is_trashed = is_trashed

        is_shared = backend.get_config(FilterState._make_is_shared_config_key(tree_id), Ternary.NOT_SPECIFIED)
        is_shared = Ternary(is_shared)
        filter_criteria.is_shared = is_shared

        show_subtrees_of_matches = ensure_bool(backend.get_config(FilterState._make_show_subtree_config_key(tree_id)))
        filter_criteria.show_subtrees_of_matches = show_subtrees_of_matches

        if filter_criteria.has_criteria():
            logger.debug(f'[{tree_id}] Read FilterCriteria: ignore_case={filter_criteria.ignore_case} is_trashed={filter_criteria.is_trashed}'
                         f' is_shared={filter_criteria.is_shared} show_subtrees={filter_criteria.show_subtrees_of_matches}')
        return FilterState(filter_criteria)
