import logging
import os
from typing import Deque, Iterable, List, Optional, Union

from constants import TreeDisplayMode
from model.has_get_children import HasGetChildren
from model.node.node import Node, SPIDNodePair
from model.node_identifier import SinglePathNodeIdentifier
from model.uid import UID
from model.display_tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


class DisplayTreeUiState:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DisplayTreeUiState
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, tree_id: str, root_sn: SPIDNodePair, root_exists: bool = True, offending_path: Optional[str] = None,
                 tree_display_mode: TreeDisplayMode = TreeDisplayMode.ONE_TREE_ALL_ITEMS, has_checkboxes: bool = False):
        if not tree_id:
            raise RuntimeError('Cannot build DisplayTreeUiState: tree_id cannot be empty!')
        self.tree_id: str = tree_id

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
    def create_change_tree_state(tree_id: str, root_sn: SPIDNodePair):
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


class DisplayTree(HasGetChildren):
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

    def get_root_identifier(self) -> SinglePathNodeIdentifier:
        return self.state.root_sn.spid

    def get_root_node(self):
        return self.state.root_sn.node

    def is_needs_manual_load(self):
        return self.state.needs_manual_load

    def set_needs_manual_load(self, needs_manual_load: bool):
        self.state.needs_manual_load = needs_manual_load

    @property
    def root_path(self) -> str:
        """Override this if root node's identifier is not SinglePathNodeIdentifier"""
        return self.state.root_sn.spid.get_single_path()

    @property
    def tree_type(self) -> int:
        return self.state.root_sn.spid.tree_type

    @property
    def uid(self):
        return self.state.root_sn.spid.uid

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

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.get_children(self.get_root_node(), filter_criteria)

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        assert parent, 'Arg "parent" cannot be null!'
        return self.backend.get_children(parent, self.tree_id, filter_criteria)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier) -> Deque[Node]:
        return self.backend.get_ancestor_list(spid, stop_at_path=self.root_path)

    def get_child_sn_list_for_root(self) -> Iterable[SPIDNodePair]:
        child_node_list: Iterable[Node] = self.get_children_for_root()
        return self._make_child_sn_list(child_node_list, self.root_path)

    def get_child_sn_list(self, parent: SPIDNodePair) -> Iterable[SPIDNodePair]:
        child_node_list: Iterable[Node] = self.get_children(parent.node)
        return self._make_child_sn_list(child_node_list, parent.spid.get_single_path())

    @staticmethod
    def _make_child_sn_list(child_node_list: Iterable[Node], parent_path: str) -> Iterable[SPIDNodePair]:
        child_sn_list: List[SPIDNodePair] = []
        for child_node in child_node_list:
            if child_node.node_identifier.is_spid():
                # no need to do extra work!
                child_sn = SPIDNodePair(child_node.node_identifier, child_node)
            else:
                child_path = os.path.join(parent_path, child_node.name)
                if child_path not in child_node.get_path_list():
                    # this means we're not following the rules
                    raise RuntimeError(f'Could not find derived path ("{child_path}") in path list ({child_node.get_path_list()}) of child!')
                child_sn = SPIDNodePair(SinglePathNodeIdentifier(child_node.uid, child_path, child_node.get_tree_type()), child_node)
            child_sn_list.append(child_sn)
        return child_sn_list

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def print_tree_contents_debug(self):
        logger.debug(f'[{self.tree_id}] Contents of tree with root "{self.get_root_sn().spid}": \n' +
                     self.backend.cacheman.show_tree(self.get_root_sn().spid))

    def __repr__(self):
        return f'DisplayTree(tree_id="{self.tree_id}" root={self.get_root_sn()})'


class NullDisplayTree(DisplayTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DisplayTree

    A DisplayTree which has no nodes and does nothing. Useful for representing a tree whose root does not exist.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, state):
        super().__init__(backend, state)
        assert not state.root_exists, f'For state: {state}'

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return []

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return []
