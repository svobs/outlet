from typing import Iterable, List, Optional, Union

from pydispatch import dispatcher
import logging

from app.backend import OutletBackend
from executor.central import CentralExecutor
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from model.uid import UID
from store.cache_manager import CacheManager
from store.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from ui import actions
from ui.tree.filter_criteria import FilterCriteria

logger = logging.getLogger(__name__)


# CLASS BackendIntegrated
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BackendIntegrated(OutletBackend):
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

        self.connect_dispatch_listener(signal=actions.ENQUEUE_UI_TASK, receiver=self.executor.submit_async_task)

        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.START_CACHEMAN, sender=actions.ID_CENTRAL_EXEC)

    def shutdown(self):
        logger.debug('Shutting down backend')
        OutletBackend.shutdown(self)  # this will disconnect the listener for SHUTDOWN_APP as well

        dispatcher.send(actions.SHUTDOWN_APP, sender=actions.ID_CENTRAL_EXEC)

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

    def request_display_tree(self, tree_id: str, user_path: str = None, spid: SinglePathNodeIdentifier = None, is_startup: bool = False) \
            -> Optional[DisplayTree]:
        state = self.cacheman.request_display_tree_ui_state(tree_id, user_path, spid, is_startup)
        if state:
            tree = state.to_display_tree(backend=self)
            return tree
        else:
            # will be sent async
            return None

    def start_subtree_load(self, tree_id: str):
        self.cacheman.enqueue_load_subtree_task(tree_id)

    def get_op_execution_play_state(self) -> bool:
        return self.executor.enable_op_execution_thread

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return self.cacheman.get_children(parent, filter_criteria)

    def get_ancestor_list(self, spid: SinglePathNodeIdentifier, stop_at_path: Optional[str] = None) -> Iterable[Node]:
        return self.cacheman.get_ancestor_list_for_single_path_identifier(spid, stop_at_path=stop_at_path)
