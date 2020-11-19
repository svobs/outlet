import logging
import os

from pydispatch import dispatcher

import ui.actions as actions
from constants import TREE_TYPE_LOCAL_DISK, TreeDisplayMode
from diff.diff_content_first import ContentFirstDiffer
from model.node_identifier import LocalNodeIdentifier, NodeIdentifier
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

    def shutdown(self):
        HasLifecycle.shutdown(self)
        logger.debug('GlobalActions shut down')

    def _on_diff_requested(self, sender, tree_con_left, tree_con_right):
        logger.debug(f'Received signal: "{actions.START_DIFF_TREES}" from "{sender}"')
        self.app.executor.submit_async_task(self.do_tree_diff, sender, tree_con_left, tree_con_right)

    @staticmethod
    def _tree_exists(node_identifier: NodeIdentifier) -> bool:
        assert isinstance(node_identifier, LocalNodeIdentifier)
        for path in node_identifier.get_path_list():
            if os.path.exists(path):
                return True
        return False

    def do_tree_diff(self, sender, tree_con_left, tree_con_right):
        stopwatch_diff_total = Stopwatch()
        actions.disable_ui(sender=sender)
        try:
            left_id: NodeIdentifier = tree_con_left.get_root_identifier()
            right_id: NodeIdentifier = tree_con_right.get_root_identifier()
            if left_id.tree_type == TREE_TYPE_LOCAL_DISK and not self._tree_exists(left_id):
                logger.info(f'Skipping diff because the left path does not exist: "{left_id.get_path_list()}"')
                actions.enable_ui(sender=self)
                return
            elif right_id.tree_type == TREE_TYPE_LOCAL_DISK and not self._tree_exists(right_id):
                logger.info(f'Skipping diff because the right path does not exist: "{right_id.get_path_list()}"')
                actions.enable_ui(sender=self)
                return

            # Load trees if not loaded - may be a long operation
            left_fmeta_tree = tree_con_left.get_tree()
            right_fmeta_tree = tree_con_right.get_tree()

            logger.debug(f'Sending START_PROGRESS_INDETERMINATE for ID: {sender}')
            dispatcher.send(actions.START_PROGRESS_INDETERMINATE, sender=sender)
            msg = 'Computing bidrectional content-first diff...'
            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=sender, msg=msg)

            stopwatch_diff = Stopwatch()
            differ = ContentFirstDiffer(left_fmeta_tree, right_fmeta_tree, self.app)
            op_tree_left, op_tree_right, = differ.diff(compare_paths_also=True)
            logger.info(f'{stopwatch_diff} Diff completed')

            dispatcher.send(actions.SET_PROGRESS_TEXT, sender=sender, msg='Populating UI trees...')

            tree_con_left.reload(new_tree=op_tree_left, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY,
                                 show_checkboxes=True)
            tree_con_right.reload(new_tree=op_tree_right, tree_display_mode=TreeDisplayMode.CHANGES_ONE_TREE_PER_CATEGORY,
                                  show_checkboxes=True)

            dispatcher.send(actions.STOP_PROGRESS, sender=sender)
            dispatcher.send(signal=actions.DIFF_TREES_DONE, sender=sender, stopwatch=stopwatch_diff_total)
        except Exception as err:
            # Clean up progress bar:
            dispatcher.send(actions.STOP_PROGRESS, sender=sender)
            actions.enable_ui(sender=self)
            if self.app.window:
                self.app.window.show_error_ui('Diff task failed due to unexpected error', repr(err))
            logger.exception(err)

