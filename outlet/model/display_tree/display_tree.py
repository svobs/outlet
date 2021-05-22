import logging
from typing import Deque, Iterable, List, Optional, Union

from constants import MAX_NUMBER_DISPLAYABLE_CHILD_NODES, TreeDisplayMode, TreeID, TreeType
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID

logger = logging.getLogger(__name__)


class DisplayTreeUiState:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DisplayTreeUiState
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, tree_id: TreeID, root_sn: SPIDNodePair, root_exists: bool = True, offending_path: Optional[str] = None,
                 tree_display_mode: TreeDisplayMode = TreeDisplayMode.ONE_TREE_ALL_ITEMS, has_checkboxes: bool = False):
        if not tree_id:
            raise RuntimeError('Cannot build DisplayTreeUiState: tree_id cannot be empty!')
        self.tree_id: TreeID = tree_id

        if not root_sn:
            raise RuntimeError('Cannot build DisplayTreeUiState: root_sn cannot be empty!')
        assert isinstance(root_sn, SPIDNodePair), f'Expected SPIDNodePair but got {type(root_sn)}'
        self.root_sn: SPIDNodePair = root_sn
        """SPIDNodePair is needed to clarify the (albeit very rare) case where the root node resolves to multiple paths.
        Each display tree can only have one root path."""

        self.root_exists: bool = root_exists
        self.offending_path: Optional[str] = offending_path
        self.needs_manual_load: bool = False
        """If True, the UI should display a "Load" button in order to kick off the backend data load. 
        If False; the backend will automatically start loading in the background."""

        self.tree_display_mode: TreeDisplayMode = tree_display_mode
        self.has_checkboxes: bool = has_checkboxes

    @staticmethod
    def create_change_tree_state(tree_id: TreeID, root_sn: SPIDNodePair):
        return DisplayTreeUiState(tree_id, root_sn, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY, has_checkboxes=True)

    def to_display_tree(self, backend):
        if self.root_exists:
            return DisplayTree(backend, self)
        else:
            return NullDisplayTree(backend, self)

    def __repr__(self):
        return f'DisplayTreeUiState(tree_id="{self.tree_id}" root_exists={self.root_exists} offending_path=' \
               f'{self.offending_path} needs_manual_load={self.needs_manual_load} root_sn={self.root_sn} ' \
               f'tree_display_mode={self.tree_display_mode.name} has_checkboxes={self.has_checkboxes}'


class DisplayTree:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS DisplayTree
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, state: DisplayTreeUiState):
        self.backend = backend
        self.state: DisplayTreeUiState = state

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def tree_id(self):
        return self.state.tree_id

    def get_root_sn(self) -> SPIDNodePair:
        return self.state.root_sn

    def get_root_spid(self) -> SinglePathNodeIdentifier:
        return self.state.root_sn.spid

    def get_root_node(self):
        return self.state.root_sn.node

    def is_needs_manual_load(self):
        return self.state.needs_manual_load

    def set_needs_manual_load(self, needs_manual_load: bool):
        self.state.needs_manual_load = needs_manual_load

    @property
    def tree_type(self) -> TreeType:
        return self.state.root_sn.spid.tree_type

    @property
    def root_path(self) -> str:
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        return self.state.root_sn.spid.get_single_path()

    @property
    def uid(self):
        return self.state.root_sn.spid.node_uid

    @property
    def root_uid(self) -> UID:
        return self.uid

    # More root meta
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    def is_root_exists(self) -> bool:
        return self.state.root_exists

    def set_root_exists(self, exists: bool):
        self.state.root_exists = exists

    def get_offending_path(self) -> Optional[str]:
        """Only present in *some* cases where root_exists() == False"""
        return self.state.offending_path

    # Getters & search
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def is_path_in_subtree(self, path_list: Union[str, List[str]]):
        if not path_list:
            raise RuntimeError('is_path_in_subtree(): full_path not provided!')

        if isinstance(path_list, list):
            for path in path_list:
                # i.e. if any paths start with
                if path.startswith(self.root_path):
                    return True
            return False

        return path_list.startswith(self.root_path)

    def get_child_list_for_root(self) -> Iterable[SPIDNodePair]:
        return self.get_child_list_for_spid(self.get_root_spid())

    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier, is_expanding_parent: bool = False) -> Iterable[SPIDNodePair]:
        # if is_expanding_parent==True, this will also tell the backend to record the parent as expanded
        assert parent_spid, 'Arg "parent_spid" cannot be null!'
        return self.backend.get_child_list(parent_spid, self.tree_id, is_expanding_parent=is_expanding_parent,
                                           max_results=MAX_NUMBER_DISPLAYABLE_CHILD_NODES)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Iterable[SPIDNodePair]:
        return self.backend.get_ancestor_list(spid, stop_at_path=self.root_path)

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] Contents of tree with root "{self.get_root_sn().spid}": \n' +
                     self.backend.cacheman.show_tree(self.get_root_sn().spid))

    def __repr__(self):
        return f'DisplayTree(tree_id="{self.tree_id}" state={self.state})'


class NullDisplayTree(DisplayTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS NullDisplayTree

    A DisplayTree which has no nodes and does nothing. Useful for representing a tree whose root does not exist.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, state):
        super().__init__(backend, state)
        assert not state.root_exists, f'For state: {state}'

    def get_child_list_for_root(self) -> Iterable[SPIDNodePair]:
        return []

    def get_child_list_for_spid(self, parent_spid: SinglePathNodeIdentifier, is_expanding_parent: bool = False) -> Iterable[SPIDNodePair]:
        return []
