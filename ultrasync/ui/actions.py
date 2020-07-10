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
COMMAND_COMPLETE = 'command-complete'

# --- Tree actions: requests ---
CALL_EXIFTOOL = 'call-exiftool'
CALL_EXIFTOOL_LIST = 'call-exiftool-list'
SHOW_IN_NAUTILUS = 'show-in-nautilus'
CALL_XDG_OPEN = 'call-xdg-open'
EXPAND_ALL = 'expand-all'
DOWNLOAD_FROM_GDRIVE = 'download-from-gdrive'
DELETE_SINGLE_FILE = 'delete-single-file'
DELETE_SUBTREE = 'delete-subtree'
SET_ROWS_CHECKED = 'set-rows-checked'
SET_ROWS_UNCHECKED = 'set-rows-unchecked'
REFRESH_SUBTREE_STATS = 'refresh-subtree-stats'
LOAD_UI_TREE = 'load-ui-tree'
"""Requests that the central cache update the stats for all nodes in the given subtree.
When done, the central cache will send the signal SUBTREE_STATS_UPDATED to notify the tree that it can redraw the displayed nodes"""

# --- Tree actions: notifications ---
LOAD_SUBTREE_STARTED = 'load-subtree-started'
NODE_EXPANSION_TOGGLED = 'node-expansion-toggled'
ROOT_PATH_UPDATED = 'root-path-updated'
NODE_UPSERTED = 'node-upserted'
NODE_REMOVED = 'node-removed'
EXIT_DIFF_MODE = 'diff-cancelled'
SUBTREE_STATS_UPDATED = 'subtree-stats-updated'
"""Indicates that the central cache has updated the stats for the subtree, and the subtree should redraw the nodes"""

DRAG_AND_DROP = 'drag-and-drop'

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
ID_CENTRAL_EXEC = 'central-executor'

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
