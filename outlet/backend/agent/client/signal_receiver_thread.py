import logging
import threading
from typing import Dict

from pydispatch import dispatcher

from backend.agent.grpc.conversion import GRPCConverter
from backend.agent.grpc.generated.Outlet_pb2 import SignalMsg, Subscribe_Request
from constants import TreeLoadState
from model.display_tree.display_tree import DisplayTree
from model.node.directory_stats import DirectoryStats
from model.node_identifier import GUID
from model.uid import UID
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
        signal = Signal(signal_msg.sig_int)
        kwargs = {}
        # TODO: convert this long conditional list into an action dict
        if signal == Signal.DISPLAY_TREE_CHANGED or signal == Signal.GENERATE_MERGE_TREE_DONE:
            display_tree_ui_state = self._converter.display_tree_ui_state_from_grpc(signal.display_tree_ui_state)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree'] = tree
        elif signal == Signal.DIFF_TREES_DONE or signal == Signal.DIFF_TREES_CANCELLED:
            display_tree_ui_state = self._converter.display_tree_ui_state_from_grpc(signal.dual_display_tree.left_tree)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['tree_left'] = tree
            display_tree_ui_state = self._converter.display_tree_ui_state_from_grpc(signal.dual_display_tree.right_tree)
            tree: DisplayTree = display_tree_ui_state.to_display_tree(backend=self.backend)
            kwargs['right_tree'] = tree
        elif signal == Signal.OP_EXECUTION_PLAY_STATE_CHANGED:
            kwargs['is_enabled'] = signal_msg.play_state.is_enabled
        elif signal == Signal.TOGGLE_UI_ENABLEMENT:
            kwargs['enable'] = signal_msg.ui_enablement.enable
        elif signal == Signal.ERROR_OCCURRED:
            kwargs['msg'] = signal_msg.error_occurred.msg
            kwargs['secondary_msg'] = signal_msg.error_occurred.secondary_msg
        elif signal == Signal.NODE_UPSERTED or signal == Signal.NODE_REMOVED:
            kwargs['sn'] = self._converter.sn_from_grpc(signal_msg.sn)
            kwargs['parent_guid'] = signal_msg.parent_guid
        elif signal == Signal.STATS_UPDATED:
            kwargs['status_msg'] = signal_msg.stats_update.status_msg
            dir_stats_dict_by_guid: Dict[GUID, DirectoryStats] = {}
            dir_stats_dict_by_uid: Dict[UID, DirectoryStats] = {}
            for dir_meta_grpc in signal_msg.stats_update.dir_meta_by_guid_list:
                dir_stats = self._converter.dir_stats_from_grpc(dir_meta_grpc.dir_meta)
                dir_stats_dict_by_guid[dir_meta_grpc.guid] = dir_stats
            for dir_meta_grpc in signal_msg.stats_update.dir_meta_by_uid_list:
                dir_stats = self._converter.dir_stats_from_grpc(dir_meta_grpc.dir_meta)
                dir_stats_dict_by_uid[dir_meta_grpc.uid] = dir_stats
            kwargs['dir_stats_dict_by_guid'] = dir_stats_dict_by_guid
            kwargs['dir_stats_dict_by_uid'] = dir_stats_dict_by_uid
        elif signal == Signal.DOWNLOAD_FROM_GDRIVE_DONE:
            kwargs['filename'] = signal_msg.download_msg.filename
        elif signal == Signal.TREE_LOAD_STATE_UPDATED:
            kwargs['tree_load_state'] = TreeLoadState(signal_msg.tree_load_update.load_state_int)
            kwargs['status_msg'] = signal_msg.tree_load_update.status_msg
        elif signal == Signal.DEVICE_UPSERTED:
            kwargs['device'] = self._converter.device_from_grpc(signal_msg.device)
        logger.info(f'Relaying locally: signal="{signal.name}" sender="{signal_msg.sender}" args={kwargs}')
        kwargs['signal'] = signal
        kwargs['sender'] = signal_msg.sender
        # IMPORTANT: Do not be tempted to use PyDispatcher's "named" argument for kwargs. It seems to fail in unexpected ways
        dispatcher.send(**kwargs)
