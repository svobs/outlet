import logging
import threading

from pydispatch import dispatcher

from be.agent.grpc.conversion import GRPCConverter
from be.agent.grpc.generated.Outlet_pb2 import SignalMsg, Subscribe_Request
from signal_constants import ID_CENTRAL_EXEC, Signal
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class SignalReceiverThread(HasLifecycle, threading.Thread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SignalReceiverThread

    This is used by the gRPC CLIENT. Listens for notifications which will be sent asynchronously by the backend gRPC server.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, converter):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self.run, name='SignalReceiverThread', daemon=True)

        self.backend = backend
        self._converter: GRPCConverter = converter
        self._shutdown: bool = False

    def start(self):
        HasLifecycle.start(self)
        threading.Thread.start(self)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        # self.dispatcher_thread.shutdown()

        if self._shutdown:
            return

        logger.debug(f'Shutting down {self.name}')
        self._shutdown = True

    def run(self):
        logger.info(f'Starting {self.name}...')

        while not self._shutdown:
            try:
                logger.info('Subscribing to signals from gRPC server...')
                self._receive_server_signals()
            except Exception as err:
                # FIXME: need to handle server connection failures
                logger.error(f'Serious gRPC connection failure (sending shutdown signal)! {repr(err)}')
                dispatcher.send(signal=Signal.SHUTDOWN_APP, sender=ID_CENTRAL_EXEC)
                return
            finally:
                logger.debug(f'{self.name} Exiting run loop.')

    def _receive_server_signals(self):
        request = Subscribe_Request()
        # this does not check whether it is connected...
        response_iter = self.backend.grpc_stub.subscribe_to_signals(request)

        while not self._shutdown:
            if not response_iter:
                logger.warning('ResponseIter is None! Killing connection')
                return
            try:
                signal = next(response_iter)  # blocks each time until signal received, or server shutdown
                if signal:
                    logger.debug(f'Got gRPC signal "{Signal(signal.sig_int).name}" from sender "{signal.sender}"')
                    try:
                        self._relay_signal_locally(signal)
                    except RuntimeError:
                        logger.exception('Unexpected error while relaying signal!')
                else:
                    logger.warning('Received None for signal! Killing connection')
                    return
            except StopIteration:
                logger.error('Server disconnected! Bailing...')
                return

    def _relay_signal_locally(self, signal_msg: SignalMsg):
        """Take the signal (received from server) and dispatch it to our UI process"""
        kwargs = self._converter.signal_from_grpc(signal_msg)
        # IMPORTANT: Do not be tempted to use PyDispatcher's "named" argument for kwargs. It seems to fail in unexpected ways
        dispatcher.send(**kwargs)
