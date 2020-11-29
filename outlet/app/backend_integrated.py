from typing import List, Optional, Union

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
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS BackendMonolith
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class BackendIntegrated(OutletBackend, HasLifecycle):
    def __init__(self, config):
        HasLifecycle.__init__(self)
        self.config = config
        self.executor: CentralExecutor = CentralExecutor(self)
        self.uid_generator: UidGenerator = PersistentAtomicIntUidGenerator(config)
        self.cacheman: CacheManager = CacheManager(self)
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)

    def start(self):
        logger.debug('Starting up backend')
        HasLifecycle.start(self)

        self.executor.start()

        self.connect_dispatch_listener(signal=actions.ENQUEUE_UI_TASK, receiver=self.executor.submit_async_task)

        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.START_CACHEMAN, sender=actions.ID_CENTRAL_EXEC)

    def shutdown(self):
        logger.debug('Shutting down backend')
        HasLifecycle.shutdown(self)

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

    def get_display_tree(self, tree_id: str, user_path: str = None, spid: SinglePathNodeIdentifier = None, is_startup: bool = False) \
            -> DisplayTree:
        state = self.cacheman.get_display_tree_ui_state(tree_id, user_path, spid, is_startup)
        tree = state.to_display_tree(backend=self)

        # Also update any listeners:
        if not is_startup:
            logger.debug(f'[{tree_id}] Firing signal: {actions.DISPLAY_TREE_CHANGED}')
            dispatcher.send(signal=actions.DISPLAY_TREE_CHANGED, sender=tree_id, tree=tree)

        return tree

    def start_subtree_load(self, tree_id: str):
        self.cacheman.enqueue_load_subtree_task(tree_id)
