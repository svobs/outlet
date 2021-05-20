
import logging
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional, Set

from pydispatch import dispatcher

from constants import IconId, TreeDisplayMode, TreeID
from model.device import Device
from model.display_tree.build_struct import DiffResultTreeIds, DisplayTreeRequest, RowsOfInterest
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.user_op import UserOp
from signal_constants import ID_GDRIVE_DIR_SELECT, Signal
from model.display_tree.filter_criteria import FilterCriteria
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
        # This works for both Application & thin client FE:
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)

    @staticmethod
    def report_error(sender: str, msg: str, secondary_msg: Optional[str] = None):
        """Convenience method for notifying the user about errors"""
        dispatcher.send(signal=Signal.ERROR_OCCURRED, sender=sender, msg=msg, secondary_msg=secondary_msg)

    @staticmethod
    def report_exception(sender: str, msg: str, error: Exception):
        """Convenience method for notifying the user about errors"""
        logger.exception(f'[{sender}] {msg}')
        dispatcher.send(signal=Signal.ERROR_OCCURRED, sender=sender, msg=msg, secondary_msg={error})

    @abstractmethod
    def get_config(self, config_key: str, default_val: Optional[str] = None, required: bool = True) -> Optional[str]:
        pass

    @abstractmethod
    def get_config_list(self, config_key_list: List[str]) -> Dict[str, str]:
        pass

    @abstractmethod
    def put_config(self, config_key: str, config_val: str):
        pass

    @abstractmethod
    def put_config_list(self, config_dict: Dict[str, str]):
        pass

    @abstractmethod
    def get_icon(self, icon_id: IconId) -> Optional:
        pass

    @abstractmethod
    def get_node_for_uid(self, uid: UID, device_uid: Optional[UID] = None) -> Optional[Node]:
        pass

    @abstractmethod
    def next_uid(self) -> UID:
        pass

    @abstractmethod
    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None) -> UID:
        pass

    @abstractmethod
    def get_sn_for(self, node_uid: UID, device_uid: UID, full_path: str) -> Optional[SPIDNodePair]:
        pass

    @abstractmethod
    def start_subtree_load(self, tree_id: TreeID):
        pass

    @abstractmethod
    def get_op_execution_play_state(self) -> bool:
        pass

    @abstractmethod
    def get_device_list(self) -> List[Device]:
        pass

    @abstractmethod
    def get_child_list(self, parent_spid: SinglePathNodeIdentifier, tree_id: TreeID, max_results: int = 0) -> Iterable[SPIDNodePair]:
        """If max_results is 0, unlimited nodes are returned. If nonzero and actual node count exceeds this, ResultsExceededError is raised"""
        pass

    @abstractmethod
    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[SPIDNodePair]:
        pass

    @abstractmethod
    def set_selected_rows(self, tree_id: TreeID, selected: Set[GUID]):
        pass

    @abstractmethod
    def remove_expanded_row(self, row_guid: GUID, tree_id: TreeID):
        pass

    @abstractmethod
    def get_rows_of_interest(self, tree_id: TreeID) -> RowsOfInterest:
        """I really could not think of a better name for this."""
        pass

    def create_display_tree_for_gdrive_select(self, device_uid: UID) -> Optional[DisplayTree]:
        spid = NodeIdentifierFactory.get_root_constant_gdrive_spid(device_uid)
        request = DisplayTreeRequest(tree_id=ID_GDRIVE_DIR_SELECT, return_async=False, spid=spid,
                                     tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS)
        return self.request_display_tree(request)

    def create_display_tree_from_config(self, tree_id: TreeID, is_startup: bool = False) -> Optional[DisplayTree]:
        # no arguments will be recognized by CacheMan as needing to read from config
        request = DisplayTreeRequest(tree_id=tree_id, return_async=False, is_startup=is_startup, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS)
        return self.request_display_tree(request)

    def create_display_tree_from_spid(self, tree_id: TreeID, spid: SinglePathNodeIdentifier) -> Optional[DisplayTree]:
        request = DisplayTreeRequest(tree_id=tree_id, return_async=True, spid=spid, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS)
        return self.request_display_tree(request)

    def create_display_tree_from_user_path(self, tree_id: TreeID, user_path: str, device_uid: UID) -> Optional[DisplayTree]:
        request = DisplayTreeRequest(tree_id=tree_id, return_async=True, user_path=user_path, device_uid=device_uid,
                                     tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS)
        return self.request_display_tree(request)

    def create_existing_display_tree(self, tree_id: TreeID, tree_display_mode: TreeDisplayMode) -> Optional[DisplayTree]:
        request = DisplayTreeRequest(tree_id=tree_id, return_async=False, tree_display_mode=tree_display_mode)
        return self.request_display_tree(request)

    @abstractmethod
    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTree]:
        """
        Notifies the backend that the tree was requested, and returns a display tree object, which the backend will also send via
        notification (unless is_startup==True, in which case no notification will be sent). Also is_startup helps determine whether
        to load it immediately.

        The DisplayTree object is immediately created and returned even if the tree has not finished loading on the backend. The backend
        will send a notification if/when it has finished loading.
        """
        pass

    @abstractmethod
    def drop_dragged_nodes(self, src_tree_id: TreeID, src_guid_list: List[GUID], is_into: bool, dst_tree_id: TreeID, dst_guid: GUID):
        pass

    @abstractmethod
    def start_diff_trees(self, tree_id_left: TreeID, tree_id_right: TreeID) -> DiffResultTreeIds:
        pass

    @abstractmethod
    def generate_merge_tree(self, tree_id_left: TreeID, tree_id_right: TreeID,
                            selected_change_list_left: List[GUID], selected_change_list_right: List[GUID]):
        pass

    @abstractmethod
    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: TreeID):
        pass

    @abstractmethod
    def enqueue_refresh_subtree_stats_task(self, root_uid: UID, tree_id: TreeID):
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

    @abstractmethod
    def get_filter_criteria(self, tree_id: TreeID) -> Optional[FilterCriteria]:
        pass

    @abstractmethod
    def update_filter_criteria(self, tree_id: TreeID, filter_criteria: FilterCriteria):
        pass
