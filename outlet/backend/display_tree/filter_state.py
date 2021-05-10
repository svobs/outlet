import logging
from collections import deque
from typing import Deque, Dict, List, Union

from constants import SUPER_DEBUG, TRACELOG_ENABLED, TrashStatus, TreeID, TreeType
from model.display_tree.filter_criteria import FilterCriteria, Ternary
from model.node.directory_stats import DirectoryStats
from model.node.node import SPIDNodePair
from model.node_identifier import GUID, SinglePathNodeIdentifier
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

    def __init__(self, filter_criteria: FilterCriteria, root_sn: SPIDNodePair):
        self.filter: FilterCriteria = filter_criteria
        self.root_sn: SPIDNodePair = root_sn
        self.cached_node_dict: Dict[GUID, List[SPIDNodePair]] = {}
        self.cached_dir_stats: Dict[Union[UID, GUID], DirectoryStats] = {}

    def has_criteria(self) -> bool:
        return self.filter.has_criteria()

    def update_root_sn(self, new_root_sn: SPIDNodePair):
        if new_root_sn.spid != self.root_sn.spid:
            self.root_sn = new_root_sn
            self.cached_node_dict.clear()
            self.cached_dir_stats.clear()

    def _matches_search_query(self, sn: SPIDNodePair) -> bool:
        return not self.filter.search_query or (self.filter.ignore_case and self.filter.search_query.lower() in sn.node.name.lower()) \
               or self.filter.search_query in sn.node.name

    def _matches_trashed(self, sn: SPIDNodePair) -> bool:
        return self.filter.is_trashed == Ternary.NOT_SPECIFIED or \
               (self.filter.is_trashed == Ternary.FALSE and sn.node.get_trashed_status() == TrashStatus.NOT_TRASHED) or \
               (self.filter.is_trashed == Ternary.TRUE and sn.node.get_trashed_status() != TrashStatus.NOT_TRASHED)

    def _matches_is_shared(self, sn: SPIDNodePair) -> bool:
        return self.filter.is_shared == Ternary.NOT_SPECIFIED or sn.node.tree_type != TreeType.GDRIVE \
               or sn.node.is_shared == bool(self.filter.is_shared)

    def matches(self, sn) -> bool:
        if not self._matches_search_query(sn):
            if TRACELOG_ENABLED:
                logger.debug(f'Search query "{self.filter.search_query}" not found in "{sn.node.name}" (ignore_case={self.filter.ignore_case})')
            return False

        if not self._matches_trashed(sn):
            if TRACELOG_ENABLED:
                logger.debug(f'Node with TrashStatus={sn.node.get_trashed_status()} does not match trashed={self.filter.is_trashed}')
            return False

        if not self._matches_is_shared(sn):
            if TRACELOG_ENABLED:
                logger.debug(f'Node with IsShared={sn.node.is_shared()} does not match shared={self.filter.is_shared}')
            return False

        return True

    def _build_cache_with_ancestors(self, parent_tree):
        assert self.filter.show_ancestors_of_matches
        subtree_root_node = self.root_sn.node
        logger.debug(f'Building filtered node dict for subroot {subtree_root_node.node_identifier}')
        node_dict: Dict[GUID, List[SPIDNodePair]] = {}
        dir_stats_dict: Dict[GUID, DirectoryStats] = {}

        dir_queue: Deque[SPIDNodePair] = deque()
        second_pass_stack: Deque[SPIDNodePair] = deque()

        if self.root_sn.node.is_dir():
            dir_queue.append(self.root_sn)
            second_pass_stack.append(self.root_sn)

        # First pass: find all directories in BFS order and append to second_pass_stack
        while len(dir_queue) > 0:
            sn: SPIDNodePair = dir_queue.popleft()

            child_list: List[SPIDNodePair] = parent_tree.get_child_list_for_spid(sn.spid)
            if child_list:
                for child_sn in child_list:
                    if child_sn.node.is_dir():
                        dir_queue.append(child_sn)
                        second_pass_stack.append(child_sn)

        # Second pass: go back up the tree, and add entries for ancestors with matching descendants:
        while len(second_pass_stack) > 0:
            parent_sn = second_pass_stack.pop()
            parent_guid = parent_sn.spid.guid
            child_list = parent_tree.get_child_list_for_spid(parent_sn.spid)
            filtered_child_list = []

            # Calculate stats also. Compare with BaseTree.generate_dir_stats()
            dir_stats = dir_stats_dict.get(parent_guid, None)
            if not dir_stats:
                dir_stats = DirectoryStats()
                dir_stats_dict[parent_guid] = dir_stats

            for child_sn in child_list:
                child_guid = child_sn.spid.guid
                # Include dirs if any of their children are included, or if they match. Include non-dirs only if they match:
                if (child_sn.node.is_dir() and child_guid in node_dict) or self.matches(child_sn):
                    filtered_child_list.append(child_sn)

                    if child_sn.node.is_dir():
                        child_stats = dir_stats_dict.get(child_guid, None)
                        if not child_stats:
                            # should never happen
                            logger.error(f'DirStatsDict contents: {dir_stats_dict}')
                            raise RuntimeError(f'Internal error: no child stats in dict for dir node: {child_guid} ({child_sn.spid})')
                        dir_stats.add_dir_stats(child_stats, child_sn.node.get_trashed_status() == TrashStatus.NOT_TRASHED)
                    else:
                        dir_stats.add_file_node(child_sn.node)
            if filtered_child_list:
                node_dict[parent_guid] = filtered_child_list

        logger.debug(f'Built filtered node dict with {len(node_dict)} entries')
        self.cached_node_dict = node_dict
        self.cached_dir_stats = dir_stats_dict

    def _build_cache_for_flat_list(self, parent_tree):
        """
        If not showing ancestors, then search results will be a big flat list.

        Builds the node cache and the dir stats.
        """
        filtered_list: List[SPIDNodePair] = []
        dir_stats = DirectoryStats()  # Treat the whole list like one dir
        queue: Deque[SPIDNodePair] = deque()
        for sn in parent_tree.get_child_list_for_spid(self.root_sn.spid):
            queue.append(sn)

        while len(queue) > 0:
            sn: SPIDNodePair = queue.popleft()

            if self.matches(sn):
                # Add to node_dict:
                filtered_list.append(sn)

                # Add to dir_stats (Compare with BaseTree.generate_dir_stats())
                if sn.node.is_dir():
                    if sn.node.get_trashed_status() == TrashStatus.NOT_TRASHED:
                        dir_stats.trashed_dir_count += 1
                    else:
                        dir_stats.dir_count += 1
                else:
                    dir_stats.add_file_node(sn.node)

            # Add next level to the queue:
            if sn.node.is_dir():
                for child_sn in parent_tree.get_child_list_for_spid(sn.spid):
                    queue.append(child_sn)

        root_guid: GUID = self.root_sn.spid.guid
        logger.info(f'Built node cache for flat list with root_guid: {root_guid}')
        # This will be the only entry in the dict:
        self.cached_node_dict[root_guid] = filtered_list
        self.cached_dir_stats[root_guid] = dir_stats

    def ensure_cache_populated(self, parent_tree):
        if self.cached_node_dict or not self.root_sn.node:
            return

        self.rebuild_cache(parent_tree)

    def rebuild_cache(self, parent_tree):
        logger.debug(f'Rebuilding cache for root: {self.root_sn.spid}')
        self.cached_node_dict.clear()
        self.cached_dir_stats.clear()

        if self.filter.show_ancestors_of_matches:
            self._build_cache_with_ancestors(parent_tree)
        else:
            self._build_cache_for_flat_list(parent_tree)

    def _hash_current_filter(self) -> str:
        return f'{int(self.filter.show_ancestors_of_matches)}:{int(self.filter.ignore_case)}:{int(self.filter.is_trashed)}:' \
               f'{int(self.filter.is_shared)}:{self.filter.search_query}'

    def get_filtered_child_list(self, parent_spid: SinglePathNodeIdentifier, parent_tree) -> List[SPIDNodePair]:
        assert parent_tree, 'parent_tree cannot be None!'
        if SUPER_DEBUG:
            logger.debug(f'get_filtered_child_list(spid={parent_spid})')
        if not self.filter.has_criteria():
            # logger.debug(f'No FilterCriteria selected; returning unfiltered list')
            return parent_tree.get_child_list_for_spid(parent_spid)

        self.ensure_cache_populated(parent_tree)

        guid = parent_spid.guid
        sn_list = self.cached_node_dict.get(guid, [])

        logger.info(f'Got {len(sn_list)} child nodes for parent GUID: {guid}')
        return sn_list

    def get_dir_stats(self) -> Dict[GUID, DirectoryStats]:
        assert self.cached_dir_stats is not None
        return self.cached_dir_stats

    @staticmethod
    def _make_search_query_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.filter.search_query'

    @staticmethod
    def _make_ignore_case_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.filter.ignore_case'

    @staticmethod
    def _make_is_trashed_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.filter.is_trashed'

    @staticmethod
    def _make_is_shared_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.filter.is_shared'

    @staticmethod
    def _make_show_subtree_config_key(tree_id: TreeID) -> str:
        return f'ui_state.{tree_id}.filter.show_subtrees'

    def write_to_config(self, backend, tree_id: TreeID):
        search_query = self.filter.search_query
        if not search_query:
            search_query = ''
        logger.debug(f'[{tree_id}] Writing FilterCriteria to app_config with search query: "{search_query}"')
        backend.put_config(FilterState._make_search_query_config_key(tree_id), search_query)

        backend.put_config(FilterState._make_ignore_case_config_key(tree_id), self.filter.ignore_case)

        backend.put_config(FilterState._make_is_trashed_config_key(tree_id), self.filter.is_trashed)

        backend.put_config(FilterState._make_is_shared_config_key(tree_id), self.filter.is_shared)

        backend.put_config(FilterState._make_show_subtree_config_key(tree_id), self.filter.show_ancestors_of_matches)

    @staticmethod
    def from_config(backend, tree_id: TreeID, root_sn: SPIDNodePair):
        assert tree_id, 'No tree_id specified!'
        logger.debug(f'[{tree_id}] Reading FilterCriteria from app_config')
        filter_criteria = FilterCriteria()

        search_query = backend.get_config(FilterState._make_search_query_config_key(tree_id), default_val='', required=False)
        filter_criteria.search_query = search_query

        ignore_case = ensure_bool(backend.get_config(FilterState._make_ignore_case_config_key(tree_id), default_val=False, required=False))
        filter_criteria.ignore_case = ignore_case

        is_trashed = backend.get_config(FilterState._make_is_trashed_config_key(tree_id), default_val=Ternary.NOT_SPECIFIED, required=False)
        is_trashed = Ternary(is_trashed)
        filter_criteria.is_trashed = is_trashed

        is_shared = backend.get_config(FilterState._make_is_shared_config_key(tree_id), default_val=Ternary.NOT_SPECIFIED, required=False)
        is_shared = Ternary(is_shared)
        filter_criteria.is_shared = is_shared

        show_ancestors_of_matches = ensure_bool(backend.get_config(FilterState._make_show_subtree_config_key(tree_id), default_val=False, required=False))
        filter_criteria.show_ancestors_of_matches = show_ancestors_of_matches

        if filter_criteria.has_criteria():
            logger.debug(f'[{tree_id}] Read FilterCriteria: ignore_case={filter_criteria.ignore_case} is_trashed={filter_criteria.is_trashed}'
                         f' is_shared={filter_criteria.is_shared} show_subtrees={filter_criteria.show_ancestors_of_matches}')
        return FilterState(filter_criteria, root_sn)
