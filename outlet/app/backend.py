
import logging
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Union

from pydispatch import dispatcher

from model.display_tree.display_tree import DisplayTree
from model.node.node import Node, SPIDNodePair
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.user_op import UserOp
from ui.signal import ID_GDRIVE_DIR_SELECT, Signal
from ui.tree.filter_criteria import FilterCriteria
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class OutletBackend(HasLifecycle, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OutletBackend
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        HasLifecycle.__init__(self)

    @staticmethod
    def report_error(sender: str, msg: str, secondary_msg: Optional[str] = None):
        """Convenience method for notifying the user about errors"""
        dispatcher.send(signal=Signal.ERROR_OCCURRED, sender=sender, msg=msg)

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
    def request_display_tree(self, tree_id: str, return_async: bool, user_path: str = None, spid: SinglePathNodeIdentifier = None,
                             is_startup: bool = False) -> Optional[DisplayTree]:
        pass

    @abstractmethod
    def start_subtree_load(self, tree_id: str):
        pass

    @abstractmethod
    def get_op_execution_play_state(self) -> bool:
        pass

    @abstractmethod
    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        pass

    @abstractmethod
    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[Node]:
        pass

    def create_display_tree_for_gdrive_select(self) -> Optional[DisplayTree]:
        spid = NodeIdentifierFactory.get_gdrive_root_constant_single_path_identifier()
        return self._create_display_tree(ID_GDRIVE_DIR_SELECT, return_async=False, spid=spid)

    def create_display_tree_from_config(self, tree_id: str, is_startup: bool = False) -> Optional[DisplayTree]:
        # no arguments will be recognized by CacheMan as needing to read from config
        return self._create_display_tree(tree_id, return_async=False, is_startup=is_startup)

    def create_display_tree_from_spid(self, tree_id: str, spid: SinglePathNodeIdentifier) -> Optional[DisplayTree]:
        return self._create_display_tree(tree_id, return_async=True, spid=spid)

    def create_display_tree_from_user_path(self, tree_id: str, user_path: str) -> Optional[DisplayTree]:
        return self._create_display_tree(tree_id, return_async=True, user_path=user_path)

    def _create_display_tree(self, tree_id: str, return_async: bool, user_path: Optional[str] = None,
                             spid: Optional[SinglePathNodeIdentifier] = None, is_startup: bool = False) -> Optional[DisplayTree]:
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

        return self.request_display_tree(tree_id, return_async, user_path, spid, is_startup)

    @abstractmethod
    def drop_dragged_nodes(self, src_tree_id: str, src_sn_list: List[SPIDNodePair], is_into: bool, dst_tree_id: str, dst_sn: SPIDNodePair):
        pass

    @abstractmethod
    def start_diff_trees(self, tree_id_left: str, tree_id_right: str):
        pass

    @abstractmethod
    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[Node]:
        pass

    @abstractmethod
    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: str):
        pass

    @abstractmethod
    def enqueue_refresh_subtree_stats_task(self, root_uid: UID, tree_id: str):
        pass

    @abstractmethod
    def get_last_pending_op(self, node_uid: UID) -> Optional[UserOp]:
        pass

    @abstractmethod
    def download_file_from_gdrive(self, node_uid: UID, requestor_id: str):
        pass

    @abstractmethod
    def delete_subtree(self, node_uid_list: List[UID]):
        pass
