import logging
from typing import Dict, Optional, Set

from backend.display_tree.change_tree import ChangeTree
from backend.display_tree.filter_state import FilterState
from constants import TreeID
from model.display_tree.display_tree import DisplayTreeUiState
from backend.display_tree.root_path_config import RootPathConfigPersister
from model.node.directory_stats import DirectoryStats
from model.node_identifier import GUID
from model.uid import UID

logger = logging.getLogger(__name__)


class ActiveDisplayTreeMeta:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS ActiveDisplayTreeMeta

    For internal use by CacheManager.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, state: DisplayTreeUiState, filter_state: FilterState):
        self.state: DisplayTreeUiState = state
        self.filter_state: FilterState = filter_state

        self.change_tree: Optional[ChangeTree] = None
        """For order > 1 only"""
        self.src_tree_id: Optional[TreeID] = None
        """For order > 1 only"""

        self.root_path_config_persister: Optional[RootPathConfigPersister] = None
        self.expanded_row_set: Set[GUID] = set()
        self.selected_row_set: Set[GUID] = set()

        self.summary_msg: Optional[str] = None
        self.dir_stats_unfiltered_by_uid: Dict[UID, DirectoryStats] = {}
        self.dir_stats_unfiltered_by_guid: Dict[GUID, DirectoryStats] = {}
        """A map containing the current stats for each dir node (with NO filter applied).
        See the filter_state for a map of stats WITH the filter applied"""

        logger.debug(f'[{self.state.tree_id}] NeedsManualLoad = {state.needs_manual_load}')

    def is_first_order(self) -> bool:
        """'First order' means the tree relies on the master caches directly.
        This implies that it is not a change tree, and that its data may not have been laoded yet"""
        return not self.change_tree

    @property
    def root_sn(self):
        return self.state.root_sn

    @property
    def tree_id(self):
        return self.state.tree_id

    @property
    def root_exists(self):
        return self.state.root_exists

    @property
    def offending_path(self):
        return self.state.offending_path
