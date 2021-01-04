import logging
import threading
import time

from pydispatch import dispatcher

from backend.daemon.grpc.conversion import Converter
from backend.daemon.grpc.generated.Outlet_pb2 import SignalMsg, Subscribe_Request
from constants import GRPC_CLIENT_REQUEST_MAX_RETRIES, GRPC_CLIENT_SLEEP_ON_FAILURE_SEC
from model.display_tree.display_tree import DisplayTree
from signal_constants import ID_CENTRAL_EXEC, Signal
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class SignalReceiverThread(HasLifecycle, threading.Thread):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SignalReceiverThread

    Listens for notifications which will be sent asynchronously by the backend gRPC server.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        threading.Thread.__init__(self, target=self.run, name='SignalReceiverThread', daemon=True)

        self.backend = backend
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

    @staticmethod
    def _try_repeatedly(request_func):
        retries_remaining = GRPC_CLIENT_REQUEST_MAX_RETRIES
        while True:
            try:
                return request_func()
            except Exception as err:
                logger.exception(f'Request failed: {repr(err)}: sleeping {GRPC_CLIENT_SLEEP_ON_FAILURE_SEC} secs (retries left: {retries_remaining})')
                if retries_remaining == 0:
                    # Fatal error: shutdown the rest of the app
                    logger.error(f'Too many failures: sending shutdown signal')
                    dispatcher.send(signal=Signal.SHUTDOWN_APP, sender=ID_CENTRAL_EXEC)
                    raise

                time.sleep(GRPC_CLIENT_SLEEP_ON_FAILURE_SEC)
                retries_remaining -= 1

    def run(self):
        logger.info(f'Starting {self.name}...')

        while not self._shutdown:
            logger.info('Subscribing to signals from server...')

            while not self._shutdown:
                logger.debug('Subscribing to GRPC signals')
                self._try_repeatedly(lambda: self._receive_server_signals())

            logger.debug('Connection to server ended.')

        logger.debug(f'{self.name} Run loop ended.')

    def _receive_server_signals(self):
        request = Subscribe_Request()
        response_iter = self.backend.grpc_stub.subscribe_to_signals(request)
        logger.debug(f'Subscribed to signals from server')

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

    def _relay_signal_locally(self, signal: SignalMsg):
        """Take the signal (received from server) and dispatch it to our UI process"""
        sig = Signal(signal.sig_int)
        kwargs = {}
        # TODO: convert this long conditional list into an action dict
        if signal.sig_int == Signal.DISPLAY_TREE_CHANGED \
                or signal.sig_int == Signal.GENERATE_MERGE_TREE_DONE:
            display_tree_ui_state = Converter.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree'] = tree
        elif signal.sig_int == Signal.OP_EXECUTION_PLAY_STATE_CHANGED:
            kwargs['is_enabled'] = signal.play_state.is_enabled
        elif signal.sig_int == Signal.TOGGLE_UI_ENABLEMENT:
            kwargs['enable'] = signal.ui_enablement.enable
        elif signal.sig_int == Signal.ERROR_OCCURRED:
            kwargs['msg'] = signal.error_occurred.msg
            kwargs['secondary_msg'] = signal.error_occurred.secondary_msg
        elif signal.sig_int == Signal.NODE_UPSERTED:
            kwargs['node'] = Converter.node_from_grpc(signal.node)
        elif signal.sig_int == Signal.NODE_REMOVED:
            kwargs['node'] = Converter.node_from_grpc(signal.node)
        elif signal.sig_int == Signal.NODE_MOVED:
            kwargs['src_node'] = Converter.node_from_grpc(signal.src_dst_node_list.src_node)
            kwargs['dst_node'] = Converter.node_from_grpc(signal.src_dst_node_list.dst_node)
        elif signal.sig_int == Signal.SET_STATUS:
            kwargs['status_msg'] = signal.status_msg.msg
        elif signal.sig_int == Signal.DOWNLOAD_FROM_GDRIVE_DONE:
            kwargs['filename'] = signal.download_msg.filename
        logger.info(f'Relaying locally: signal="{sig.name}" sender="{signal.sender}" args={kwargs}')
        kwargs['signal'] = sig
        kwargs['sender'] = signal.sender
        # IMPORTANT: Do not be tempted to use PyDispatcher's "named" argument for kwargs. It seems to fail in unexpected ways
        dispatcher.send(**kwargs)