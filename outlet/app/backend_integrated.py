from typing import Iterable, List, Optional

from pydispatch import dispatcher
import logging

from app.backend import DiffResultTreeIds, DisplayTreeRequest, OutletBackend
from executor.central import CentralExecutor
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node, SPIDNodePair
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from model.user_op import UserOp
from store.cache_manager import CacheManager
from store.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from task.tree_diff import TreeDiffAction
from ui.signal import ID_CENTRAL_EXEC, Signal
from model.display_tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


class BackendIntegrated(OutletBackend):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS BackendIntegrated
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, config):
        OutletBackend.__init__(self)
        self.config = config
        self.executor: CentralExecutor = CentralExecutor(self)
        self.uid_generator: UidGenerator = PersistentAtomicIntUidGenerator(config)
        self.cacheman: CacheManager = CacheManager(self)
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)

    def start(self):
        logger.debug('Starting up backend')
        OutletBackend.start(self)

        self.executor.start()

        self.connect_dispatch_listener(signal=Signal.ENQUEUE_UI_TASK, receiver=self.executor.submit_async_task)

        # Kick off cache load now that we have a progress bar
        dispatcher.send(Signal.START_CACHEMAN, sender=ID_CENTRAL_EXEC)

    def shutdown(self):
        logger.debug('Shutting down backend')
        OutletBackend.shutdown(self)  # this will disconnect the listener for SHUTDOWN_APP as well

        dispatcher.send(Signal.SHUTDOWN_APP, sender=ID_CENTRAL_EXEC)

        self.cacheman = None
        self.executor = None

    def get_node_for_uid(self, uid: UID, tree_type: int = None) -> Optional[Node]:
        return self.cacheman.get_node_for_uid(uid, tree_type)

    def get_node_for_local_path(self, full_path: str) -> Optional[Node]:
        return self.cacheman.get_node_for_local_path(full_path)

    def next_uid(self) -> UID:
        return self.uid_generator.next_uid()

    def get_uid_for_local_path(self, full_path: str, uid_suggestion: Optional[UID] = None, override_load_check: bool = False) -> UID:
        return self.cacheman.get_uid_for_local_path(full_path, uid_suggestion)

    def request_display_tree(self, request: DisplayTreeRequest) -> Optional[DisplayTree]:
        assert request.tree_id, f'tree_id cannot be null for {request}'
        state = self.cacheman.request_display_tree_ui_state(request)
        if state:
            tree = state.to_display_tree(backend=self)
            return tree
        else:
            # will be sent async
            assert request.return_async, f'No tree and return_async==False for {request.tree_id}'
            return None

    def start_subtree_load(self, tree_id: str):
        self.cacheman.enqueue_load_subtree_task(tree_id, send_signals=True)

    def get_op_execution_play_state(self) -> bool:
        return self.executor.enable_op_execution_thread

    def get_children(self, parent: Node, tree_id: Optional[str], filter_criteria: Optional[FilterCriteria] = None) -> Iterable[Node]:
        return self.cacheman.get_children(parent, tree_id, filter_criteria)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[Node]:
        return self.cacheman.get_ancestor_list_for_spid(spid, stop_at_path=stop_at_path)

    def drop_dragged_nodes(self, src_tree_id: str, src_sn_list: List[SPIDNodePair], is_into: bool, dst_tree_id: str, dst_sn: SPIDNodePair):
        self.cacheman.drop_dragged_nodes(src_tree_id, src_sn_list, is_into, dst_tree_id, dst_sn)

    def start_diff_trees(self, tree_id_left: str, tree_id_right: str) -> DiffResultTreeIds:
        return self.executor.start_tree_diff(tree_id_left, tree_id_right)

    def generate_merge_tree(self, tree_id_left: str, tree_id_right: str,
                            selected_changes_left: List[SPIDNodePair], selected_changes_right: List[SPIDNodePair]):
        new_tree_ids = DiffResultTreeIds(f'{tree_id_left}_merged', f'{tree_id_right}_merged')
        return TreeDiffAction.generate_merge_tree(self, ID_CENTRAL_EXEC, tree_id_left, tree_id_right, new_tree_ids,
                                                  selected_changes_left, selected_changes_right)

    def enqueue_refresh_subtree_task(self, node_identifier: NodeIdentifier, tree_id: str):
        self.cacheman.enqueue_refresh_subtree_task(node_identifier, tree_id)

    def enqueue_refresh_subtree_stats_task(self, root_uid: UID, tree_id: str):
        self.cacheman.enqueue_refresh_subtree_stats_task(root_uid, tree_id)

    def get_last_pending_op(self, node_uid: UID) -> Optional[UserOp]:
        return self.cacheman.get_last_pending_op_for_node(node_uid)

    def download_file_from_gdrive(self, node_uid: UID, requestor_id: str):
        self.cacheman.download_file_from_gdrive(node_uid, requestor_id)

    def delete_subtree(self, node_uid_list: List[UID]):
        self.cacheman.delete_subtree(node_uid_list)
