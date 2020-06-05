from pydispatch import dispatcher
import logging

# Tasks
LOAD_ALL_CACHES = 'load-all-caches'
LOAD_ALL_CACHES_DONE = 'load-all-caches-done'
START_DIFF_TREES = 'start-tree-diff'
DIFF_TREES_DONE = 'tree-diff_done'
SHOW_GDRIVE_ROOT_DIALOG = 'show-gdrive-root-dialog'
DOWNLOAD_GDRIVE_META = 'download-gdrive-meta'
GDRIVE_DOWNLOAD_COMPLETE = 'gdrive-download-complete'

# --- Tree actions ---
LOAD_TREE_STARTED = 'load-tree-started'
NODE_EXPANSION_TOGGLED = 'node-expansion-toggled'
ROOT_PATH_UPDATED = 'root-path-updated'
NODE_UPDATED = 'node-updated'
NODE_ADDED = 'node-added'
NODE_REMOVED = 'node-removed'
DIFF_CANCELLED = 'diff-cancelled'

# All components should listen for this
TOGGLE_UI_ENABLEMENT = 'toggle-ui-enablement'

# --- Progress bar ---
START_PROGRESS_INDETERMINATE = 'start-progress-indeterminate'
START_PROGRESS = 'start-progress'
SET_PROGRESS_TEXT = 'set-progress-text'
PROGRESS_MADE = 'progress_made'
STOP_PROGRESS = 'stop-progress'

# --- Status bar ---
SET_STATUS = 'set-status'

# --- Sender identifiers ---
ID_DIFF_WINDOW = 'diff_win'
ID_LEFT_TREE = 'left_tree'
ID_RIGHT_TREE = 'right_tree'
ID_MERGE_TREE = 'merge_tree'
ID_GDRIVE_DIR_SELECT = 'gdrive_dir_select'
ID_GLOBAL_CACHE = 'global_cache'
ID_COMMAND_EXECUTOR = 'command-executor'

logger = logging.getLogger(__name__)


def get_dispatcher():
    return dispatcher


def send_signal(signal, sender):
    """
    (Convenience method)
    Send a given signal from the given sender, with no additional args.
    """
    dispatcher.send(signal=signal, sender=sender)


def connect(signal, handler, sender=dispatcher.Any):
    dispatcher.connect(handler, signal=signal, sender=sender)


def disable_ui(sender):
    logger.debug(f'Sender "{sender}" requested to disable the UI')
    dispatcher.send(signal=TOGGLE_UI_ENABLEMENT, sender=sender, enable=False)


def enable_ui(sender):
    dispatcher.send(signal=TOGGLE_UI_ENABLEMENT, sender=sender, enable=True)


def set_status(sender, status_msg):
    assert status_msg
    assert type(sender) == str
    dispatcher.send(signal=SET_STATUS, sender=sender, status_msg=status_msg)

# --- Tree actions ---
