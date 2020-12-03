
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
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
    def request_display_tree(self, tree_id: str, user_path: str = None, spid: SinglePathNodeIdentifier = None, is_startup: bool = False) \
            -> Optional[DisplayTree]:
        pass

    @abstractmethod
    def start_subtree_load(self, tree_id: str):
        pass

    @abstractmethod
    def get_op_execution_play_state(self) -> bool:
        pass

    def create_display_tree_for_gdrive_select(self) -> Optional[DisplayTree]:
        spid = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        return self._create_display_tree(actions.ID_GDRIVE_DIR_SELECT, spid=spid)

    def create_display_tree_from_config(self, tree_id: str, is_startup: bool = False) -> Optional[DisplayTree]:
        # no arguments will be recognized by CacheMan as needing to read from config
        return self._create_display_tree(tree_id, is_startup=is_startup)

    def create_display_tree_from_spid(self, tree_id: str, spid: SinglePathNodeIdentifier) -> Optional[DisplayTree]:
        return self._create_display_tree(tree_id, spid=spid)

    def create_display_tree_from_user_path(self, tree_id: str, user_path: str) -> Optional[DisplayTree]:
        return self._create_display_tree(tree_id, user_path=user_path)

    def _create_display_tree(self, tree_id: str, user_path: Optional[str] = None, spid: Optional[SinglePathNodeIdentifier] = None,
                             is_startup: bool = False) -> Optional[DisplayTree]:
        """
        Notifies the backend that the tree was requested, and returns a display tree object, which the backend will also send via
        notification (unless is_startup==True, in which case no notification will be sent). Also is_startup helps determine whether
        to load it immediately.

        The DisplayTree object is immediately created and returned even if the tree has not finished loading on the backend. The backend
        will send a notification if/when it has finished loading.
        """
        logger.debug(f'[{tree_id}] Got request to load display tree (user_path="{user_path}", spid={spid}, is_startup={is_startup}')
        if spid:
            assert isinstance(spid, SinglePathNodeIdentifier), f'Expected SinglePathNodeIdentifier but got {type(spid)}'
            spid.normalize_paths()

        return self.request_display_tree(tree_id, user_path, spid, is_startup)

    # def to_display_tree(self, state: DisplayTreeUiState) -> DisplayTree:
