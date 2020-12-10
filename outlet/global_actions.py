import logging
import os

from pydispatch import dispatcher

from ui.signal import ID_CENTRAL_EXEC, Signal
from constants import TREE_TYPE_LOCAL_DISK
from diff.diff_content_first import ContentFirstDiffer
from model.node.node import SPIDNodePair
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


class GlobalActions(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS GlobalActions
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        HasLifecycle.__init__(self)
        self.backend = backend

    def start(self):
        logger.debug('Starting GlobalActions listeners')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.START_DIFF_TREES, receiver=self._on_diff_requested)

    def _on_diff_requested(self, sender, tree_id_left, tree_id_right):
        logger.debug(f'Received signal: "{Signal.START_DIFF_TREES.name}" from "{sender}"')
        self.backend.executor.submit_async_task(self.do_tree_diff, sender, tree_id_left, tree_id_right)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        logger.debug('GlobalActions shut down')

    @staticmethod
    def _tree_exists_on_disk(node_identifier: NodeIdentifier) -> bool:
        assert isinstance(node_identifier, LocalNodeIdentifier)
        for path in node_identifier.get_path_list():
            if os.path.exists(path):
                return True
        return False

    def do_tree_diff(self, sender, tree_id_left: str, tree_id_right: str):
        stopwatch_diff_total = Stopwatch()
        try:
            meta_left = self.backend.cacheman.get_active_display_tree_meta(tree_id_left)
            meta_right = self.backend.cacheman.get_active_display_tree_meta(tree_id_right)
            assert meta_left and meta_right, f'Missing tree meta! Left={meta_left}, Right={meta_right}'
            left_root_sn: SPIDNodePair = meta_left.root_sn
            right_root_sn: SPIDNodePair = meta_right.root_sn
            if left_root_sn.spid.tree_type == TREE_TYPE_LOCAL_DISK and not self._tree_exists_on_disk(left_root_sn.spid):
                logger.info(f'Skipping diff because the left path does not exist: "{left_root_sn.spid.get_path_list()}"')
                dispatcher.send(signal=Signal.DIFF_TREES_FAILED, sender=sender)
                return
            elif right_root_sn.spid.tree_type == TREE_TYPE_LOCAL_DISK and not self._tree_exists_on_disk(right_root_sn.spid):
                logger.info(f'Skipping diff because the right path does not exist: "{right_root_sn.spid.get_path_list()}"')
                dispatcher.send(signal=Signal.DIFF_TREES_FAILED, sender=sender)
                return

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {sender}')
            dispatcher.send(Signal.START_PROGRESS_INDETERMINATE, sender=sender)
            msg = 'Computing bidrectional content-first diff...'
            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=sender, msg=msg)

            stopwatch_diff = Stopwatch()
            differ = ContentFirstDiffer(left_root_sn, right_root_sn, self.backend)
            op_tree_left, op_tree_right, = differ.diff(compare_paths_also=True)
            logger.info(f'{stopwatch_diff} Diff completed')

            dispatcher.send(Signal.SET_PROGRESS_TEXT, sender=sender, msg='Populating UI trees...')

            # TODO: 0. Make gRPC return a new display_tree and then start waiting for Signal.DIFF_ONE_SIDE_RESULT
            # TODO: 1. store display tree with new

            # Send each side's result to its UI tree:
            dispatcher.send(Signal.DIFF_ONE_SIDE_RESULT, sender=tree_id_left, new_tree=op_tree_left)
            dispatcher.send(Signal.DIFF_ONE_SIDE_RESULT, sender=tree_id_right, new_tree=op_tree_right)
            # Send general notification that we are done:
            dispatcher.send(Signal.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=Signal.DIFF_TREES_DONE, sender=sender)
            logger.debug(f'Diff time: {stopwatch_diff_total}')
        except Exception as err:
            # Clean up progress bar:
            dispatcher.send(Signal.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=Signal.DIFF_TREES_FAILED, sender=sender)
            logger.exception(err)
            GlobalActions.display_error_in_ui(ID_CENTRAL_EXEC, 'Diff task failed due to unexpected error', repr(err))

    @staticmethod
    def display_error_in_ui(sender: str, msg: str, secondary_msg: str = None):
        logger.debug(f'Sender "{sender}" sent an error msg to display')
        dispatcher.send(signal=Signal.ERROR_OCCURRED, sender=sender, msg=msg, secondary_msg=secondary_msg)

    @staticmethod
    def disable_ui(sender):
        logger.debug(f'Sender "{sender}" requested to disable the UI')
        dispatcher.send(signal=Signal.TOGGLE_UI_ENABLEMENT, sender=sender, enable=False)

    @staticmethod
    def enable_ui(sender):
        logger.debug(f'Sender "{sender}" requested to enable the UI')
        dispatcher.send(signal=Signal.TOGGLE_UI_ENABLEMENT, sender=sender, enable=True)
