from pydispatch import dispatcher
import logging

# Tasks
LOAD_ALL_CACHES = 'load-all-caches'
LOAD_ALL_CACHES_DONE = 'load-all-caches-done'
START_DIFF_TREES = 'start-diff-trees'
DIFF_TREES_DONE = 'diff-trees_done'
DOWNLOAD_GDRIVE_META = 'download-gdrive-meta'
GDRIVE_DOWNLOAD_COMPLETE = 'gdrive-download-complete'

# --- Tree actions ---
NODE_EXPANSION_TOGGLED = 'node-expansion-toggled'
ROOT_PATH_UPDATED = 'root-path-updated'

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
ID_GLOBAL_CACHE = 'global_cache'

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
    assert type(sender) == str
    dispatcher.send(signal=SET_STATUS, sender=sender, status_msg=status_msg)

# --- Tree actions ---