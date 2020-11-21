import sys
from concurrent import futures
import logging

import grpc
from pydispatch import dispatcher

from app_config import AppConfig
from daemon.grpc import Outlet_pb2_grpc
from daemon.grpc.Outlet_pb2 import PingResponse
from daemon.grpc.Outlet_pb2_grpc import OutletServicer
from executor.central import CentralExecutor
from model.node_identifier_factory import NodeIdentifierFactory
from store.cache_manager import CacheManager
from store.uid.uid_generator import PersistentAtomicIntUidGenerator, UidGenerator
from ui import actions

logger = logging.getLogger(__name__)


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(OutletServicer):
    def __init__(self, config):
        self.config = config
        self.executor: CentralExecutor = CentralExecutor(self)
        self.uid_generator: UidGenerator = PersistentAtomicIntUidGenerator(self.config)
        self.node_identifier_factory: NodeIdentifierFactory = NodeIdentifierFactory(self)
        self.cacheman: CacheManager = CacheManager(self)

    def start(self):
        self.executor.start()
        # Kick off cache load now that we have a progress bar
        dispatcher.send(actions.START_CACHEMAN, sender=actions.ID_CENTRAL_EXEC)

    def shutdown(self):
        logger.info('Shutting down app')

        dispatcher.send(actions.SHUTDOWN_APP, sender=actions.ID_CENTRAL_EXEC)

        self.cacheman = None
        self.executor = None

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        logger.info('gRPC server starting...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()
        logger.info('gRPC server stopped!')

    def ping(self, request, context):
        logger.info(f'Got ping!')
        response = PingResponse()
        response.timestamp = 1000
        return response


def main():
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        config = AppConfig(sys.argv[1])
    else:
        config = AppConfig()

    logger.info(f'Creating OutletDameon')
    daemon = OutletDaemon(config)
    daemon.start()

    try:
        daemon.serve()
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info('Caught KeyboardInterrupt. Quitting')
        daemon.shutdown()


if __name__ == '__main__':
    main()
