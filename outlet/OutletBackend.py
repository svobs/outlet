from pydispatch import dispatcher
import logging


from executor.central import CentralExecutor
from model.node_identifier_factory import NodeIdentifierFactory
from store.cache_manager import CacheManager
from store.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from ui import actions

logger = logging.getLogger(__name__)


# CLASS OutletBackend
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletBackend:
    def __init__(self, config):
        self.executor: CentralExecutor = CentralExecutor(self)
        self.uid_generator: UidGenerator = PersistentAtomicIntUidGenerator(config)
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)
        self.cacheman: CacheManager = CacheManager(self)

    def start(self):
        logger.debug('Starting up backend')
        self.executor.start()
        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.START_CACHEMAN, sender=actions.ID_CENTRAL_EXEC)

    def shutdown(self):
        logger.debug('Shutting down backend')

        dispatcher.send(actions.SHUTDOWN_APP, sender=actions.ID_CENTRAL_EXEC)

        self.cacheman = None
        self.executor = None
