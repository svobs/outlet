import logging
import os

from pydispatch import dispatcher

import ui.actions as actions
from constants import TREE_TYPE_LOCAL_DISK
from diff.diff_content_first import ContentFirstDiffer
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier, SinglePathNodeIdentifier
from util.has_lifecycle import HasLifecycle
from util.stopwatch_sec import Stopwatch

logger = logging.getLogger(__name__)


# CLASS GlobalActions
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GlobalActions(HasLifecycle):
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app

    def start(self):
        logger.debug('Starting GlobalActions listeners')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=actions.START_DIFF_TREES, receiver=self._on_diff_requested)

    def _on_diff_requested(self, sender, tree_id_left, tree_id_right):
        logger.debug(f'Received signal: "{actions.START_DIFF_TREES}" from "{sender}"')
        self.app.executor.submit_async_task(self.do_tree_diff, sender, tree_id_left, tree_id_right)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        logger.debug('GlobalActions shut down')

    @staticmethod
    def _tree_exists(node_identifier: NodeIdentifier) -> bool:
        assert isinstance(node_identifier, LocalNodeIdentifier)
        for path in node_identifier.get_path_list():
            if os.path.exists(path):
                return True
        return False

    def do_tree_diff(self, sender, tree_id_left, tree_id_right):
        stopwatch_diff_total = Stopwatch()
        try:
            meta_left = self.app.cacheman.get_active_display_tree_meta(tree_id_left)
            meta_right = self.app.cacheman.get_active_display_tree_meta(tree_id_right)
            assert meta_left and meta_right, f'Missing tree meta! Left={meta_left}, Right={meta_right}'
            left_root_spid: SinglePathNodeIdentifier = meta_left.root_identifier
            right_root_spid: SinglePathNodeIdentifier = meta_right.root_identifier
            if left_root_spid.tree_type == TREE_TYPE_LOCAL_DISK and not self._tree_exists(left_root_spid):
                logger.info(f'Skipping diff because the left path does not exist: "{left_root_spid.get_path_list()}"')
                GlobalActions.enable_ui(sender=self)
                return
            elif right_root_spid.tree_type == TREE_TYPE_LOCAL_DISK and not self._tree_exists(right_root_spid):
                logger.info(f'Skipping diff because the right path does not exist: "{right_root_spid.get_path_list()}"')
                GlobalActions.enable_ui(sender=self)
                return

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {sender}')
            dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=sender)
            msg = 'Computing bidrectional content-first diff...'
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=sender, msg=msg)

            stopwatch_diff = Stopwatch()
            differ = ContentFirstDiffer(left_root_spid, right_root_spid, self.app)
            op_tree_left, op_tree_right, = differ.diff(compare_paths_also=True)
            logger.info(f'{stopwatch_diff} Diff completed')

            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=sender, msg='Populating UI trees...')

            # Send each side's result to its UI tree:
            dispatcher.send(actions.DIFF_ONE_SIDE_RESULT, sender=tree_id_left, new_tree=op_tree_left)
            dispatcher.send(actions.DIFF_ONE_SIDE_RESULT, sender=tree_id_right, new_tree=op_tree_right)
            # Send general notification that we are done:
            dispatcher.send(actions.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=actions.DIFF_TREES_DONE, sender=sender, stopwatch=stopwatch_diff_total)
        except Exception as err:
            # Clean up progress bar:
            dispatcher.send(actions.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=actions.DIFF_TREES_FAILED, sender=sender)
            if self.app.window:
                self.app.window.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

    @staticmethod
    def disable_ui(sender):
        logger.debug(f'Sender "{sender}" requested to disable the UI')
        dispatcher.send(signal=actions.TOGGLE_UI_ENABLEMENT, sender=sender, enable=False)

    @staticmethod
    def enable_ui(sender):
        logger.debug(f'Sender "{sender}" requested to enable the UI')
        dispatcher.send(signal=actions.TOGGLE_UI_ENABLEMENT, sender=sender, enable=True)
