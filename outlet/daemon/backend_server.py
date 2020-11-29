import asyncio
import logging
import time
from concurrent import futures

import grpc

from app.backend_integrated import BackendIntegrated
from constants import GRPC_SERVER_ADDRESS, GRPC_SERVER_MAX_WORKER_THREADS
from daemon.grpc import Outlet_pb2_grpc
from daemon.outlet_grpc_service import OutletGRPCService
from model.display_tree.display_tree import DisplayTree
from ui import actions

logger = logging.getLogger(__name__)


# CLASS OutletDaemon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletDaemon(BackendIntegrated):
    def __init__(self, config):
        self.config = config
        BackendIntegrated.__init__(self, config)
        self._grpc_service = OutletGRPCService(self)

    def start(self):
        BackendIntegrated.start(self)

        self.connect_dispatch_listener(signal=actions.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed)

    def shutdown(self):
        BackendIntegrated.shutdown(self)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_SERVER_MAX_WORKER_THREADS))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self._grpc_service, server)
        server.add_insecure_port(GRPC_SERVER_ADDRESS)
        logger.info('gRPC server starting...')
        server.start()
        logger.info('gRPC server started!')
        server.wait_for_termination()  # <- blocks
        logger.info('gRPC server stopped!')

    def _on_display_tree_changed(self, sender, tree: DisplayTree):
        # This is called when the backend sends a signal to itself. Need to send to client
        self._relay_signal_to_client(actions.DISPLAY_TREE_CHANGED, tree)

    def _relay_signal_to_client(self, signal, *args):
        # TODO: gRPC
        pass


async def say_after(delay, what):
    await asyncio.sleep(delay)
    print(what)


async def do_thing():
    task1 = asyncio.create_task(
        say_after(1, 'hello'))

    task2 = asyncio.create_task(
        say_after(2, 'world'))

    task3 = asyncio.create_task(
        say_after(1, 'test'))

    await task2
    print(f"started at {time.strftime('%X')}")

    # Wait until both tasks are completed (should take
    # around 2 seconds.)
    print(f"after first: {time.strftime('%X')}")
    await task1
    print(f"after second: {time.strftime('%X')}")

    print(f"finished at {time.strftime('%X')}")


def main():
    asyncio.run(do_thing())


if __name__ == '__main__':
    main()


