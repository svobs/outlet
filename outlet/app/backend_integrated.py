from typing import List, Union

from pydispatch import dispatcher
import logging

from app.backend import OutletBackend
from executor.central import CentralExecutor
from model.node.node import Node
from model.node_identifier import NodeIdentifier
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
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)
        self.cacheman: CacheManager = CacheManager(self)

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

    def read_single_node_from_disk_for_path(self, full_path: str, tree_type: int) -> Node:
        return self.cacheman.read_single_node_from_disk_for_path(full_path, tree_type)

    def build_identifier(self, tree_type: int = None, path_list: Union[str, List[str]] = None, uid: UID = None,
                         must_be_single_path: bool = False) -> NodeIdentifier:
        return self.node_identifier_factory.for_values(tree_type, path_list, uid, must_be_single_path)
