import logging
from typing import Dict, Optional, Set

from be.disp_tree.change_tree import ChangeTree
from be.disp_tree.filter_state import FilterState
from constants import TreeID, TreeLoadState
from model.disp_tree.display_tree import DisplayTreeUiState
from be.disp_tree.root_path_config import RootPathConfigPersister
from model.node.dir_stats import DirStats
from model.node_identifier import GUID
from model.uid import UID
from signal_constants import ID_MERGE_TREE

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

        self.load_state: TreeLoadState = TreeLoadState.NOT_LOADED
        self.filter_state: FilterState = filter_state

        self.change_tree: Optional[ChangeTree] = None
        """For order > 1 only"""
        self.src_tree_id: Optional[TreeID] = None
        """For order > 1 only"""

        self.root_path_config_persister: Optional[RootPathConfigPersister] = None
        self.expanded_row_set: Set[GUID] = set()
        self.selected_row_set: Set[GUID] = set()

        self.summary_msg: Optional[str] = None
        self.dir_stats_unfiltered_by_uid: Dict[UID, DirStats] = {}
        self.dir_stats_unfiltered_by_guid: Dict[GUID, DirStats] = {}
        """A map containing the current stats for each dir node (with NO filter applied).
        See the filter_state for a map of stats WITH the filter applied"""

        # The last time, in millis, that the selection was updated. Used to ensure continuity of selection
        self.last_select_time_ms: int = 0

    def is_first_order(self) -> bool:
        """'First order' means the tree relies on the master caches directly.
        This implies that it is not a change tree, and that its data may not have been laoded yet"""
        return not self.change_tree

    def can_change_root(self) -> bool:
        # Kind of a kludge right now. The FE and BE both determine this separately
        return not (self.change_tree and self.tree_id == ID_MERGE_TREE)

    def has_checkboxes(self) -> bool:
        # Kind of a kludge right now. The FE and BE both determine this separately
        return self.change_tree and self.tree_id != ID_MERGE_TREE

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

    def __repr__(self):
        return f'ActiveDisplayTreeMeta(loaded={self.load_state} state={self.state})'
