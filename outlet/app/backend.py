
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from pydispatch import dispatcher

from model.display_tree.display_tree import DisplayTree
from model.display_tree.null import NullDisplayTree
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from store.cache_manager import DisplayTreeUiState
from ui import actions

logger = logging.getLogger(__name__)


# CLASS OutletBackend
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletBackend(ABC):
    @abstractmethod
    def get_node_for_uid(self, uid: UID, tree_type: int = None) -> Optional[Node]:
        pass

    @abstractmethod
    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        pass

    @abstractmethod
    def next_uid(self) -> UID:
        pass

    @abstractmethod
    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        pass

    @abstractmethod
    def get_display_tree_ui_state(self, tree_id: str, user_path: str = None, spid: SinglePathNodeIdentifier = None,
                                  is_startup: bool = False) -> DisplayTreeUiState:
        pass

    @abstractmethod
    def start_subtree_load(self, tree_id: str):
        pass

    def create_display_tree_for_gdrive_select(self):
        spid = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        return self._create_display_tree(actions.ID_GDRIVE_DIR_SELECT, spid=spid)

    def create_display_tree_from_config(self, tree_id: str, is_startup: bool = False) -> DisplayTree:
        # no arguments will be recognized by CacheMan as needing to read from config
        return self._create_display_tree(tree_id, is_startup=is_startup)

    def create_display_tree_from_spid(self, tree_id: str, spid: SinglePathNodeIdentifier):
        return self._create_display_tree(tree_id, spid=spid)

    def create_display_tree_from_user_path(self, tree_id: str, user_path: str) -> DisplayTree:
        return self._create_display_tree(tree_id, user_path=user_path)

    def _create_display_tree(self, tree_id: str, user_path: Optional[str] = None, spid: Optional[SinglePathNodeIdentifier] = None,
                             is_startup: bool = False) -> DisplayTree:
        """
        Performs a read-through retreival of all the nodes in the given subtree.
        """
        logger.debug(f'[{tree_id}] Got request to load subtree (user_path="{user_path}", spid={spid}, is_startup={is_startup}')
        if spid:
            assert isinstance(spid, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(spid)}'
            spid.normalize_paths()

        state = self.get_display_tree_ui_state(tree_id, user_path, spid, is_startup)
        if state.root_exists:
            tree = DisplayTree(self, state)
        else:
            tree = NullDisplayTree(self, state)

        # Also update any listeners:
        logger.debug(f'[{tree_id}] Firing signal: {actions.DISPLAY_TREE_CHANGED}')
        dispatcher.send(signal=actions.DISPLAY_TREE_CHANGED, sender=tree_id, tree=tree)

        return tree
