from pydispatch import dispatcher
import logging

# Tasks
ENQUEUE_UI_TASK = 'enqueue-ui-task'
START_CACHEMAN = 'start-cacheman'
START_OP_EXEC_THREAD = 'start-op-execution-thread'
LOAD_REGISTRY_DONE = 'load-registry-done'
START_CACHEMAN_DONE = 'start-cacheman-done'
START_DIFF_TREES = 'start-tree-diff'
DIFF_TREES_DONE = 'tree-diff_done'
DIFF_TREES_FAILED = 'tree-diff_failed'
DIFF_ONE_SIDE_RESULT = 'tree-diff_1side_result'
SYNC_GDRIVE_CHANGES = 'sync-gdrive-changes'
SHOW_GDRIVE_CHOOSER_DIALOG = 'show-gdrive-dir-chooser-dialog'
DOWNLOAD_ALL_GDRIVE_META = 'download-all-gdrive-meta'
GDRIVE_CHOOSER_DIALOG_LOAD_DONE = 'gdrive-dir-chooser-load-done'
COMMAND_COMPLETE = 'command-complete'

# --- Tree actions: requests ---
CALL_EXIFTOOL = 'call-exiftool'
CALL_EXIFTOOL_LIST = 'call-exiftool-list'
SHOW_IN_NAUTILUS = 'show-in-nautilus'
CALL_XDG_OPEN = 'call-xdg-open'
EXPAND_AND_SELECT_NODE = 'expand-select-node'
EXPAND_ALL = 'expand-all'
DOWNLOAD_FROM_GDRIVE = 'download-from-gdrive'
DELETE_SINGLE_FILE = 'delete-single-file'
DELETE_SUBTREE = 'delete-subtree'
SET_ROWS_CHECKED = 'set-rows-checked'
SET_ROWS_UNCHECKED = 'set-rows-unchecked'
REFRESH_SUBTREE_STATS = 'refresh-subtree-stats'
REFRESH_SUBTREE = 'refresh-subtree'
FILTER_UI_TREE = 'filter-ui-tree'
POPULATE_UI_TREE = 'load-ui-tree'
"""Requests that the central cache update the stats for all nodes in the given subtree.
When done, the central cache will send the signal REFRESH_SUBTREE_STATS_DONE to notify the tree that it can redraw the displayed nodes"""
SHUTDOWN_APP = 'shutdown-app'
DEREGISTER_DISPLAY_TREE = 'deregister-display-tree'

# --- Tree actions: notifications ---
LOAD_SUBTREE_STARTED = 'load-subtree-started'
NODE_EXPANSION_TOGGLED = 'node-expansion-toggled'
NODE_EXPANSION_DONE = 'node-expansion-done'
ROOT_PATH_UPDATED = 'root-path-updated'
GDRIVE_RELOADED = 'gdrive-reloaded'
NODE_UPSERTED = 'node-upserted'
NODE_REMOVED = 'node-removed'
NODE_MOVED = 'node-moved'
EXIT_DIFF_MODE = 'diff-cancelled'
ERROR_OCCURRED = 'error-occurred'
REFRESH_SUBTREE_STATS_DONE = 'refresh-subtree-stats-done'
REFRESH_SUBTREE_STATS_COMPLETELY_DONE = 'refresh-subtree-stats-completely-done'
REFRESH_SUBTREE_DONE = 'refresh-subtree-done'
"""Indicates that the central cache has updated the stats for the subtree, and the subtree should redraw the nodes"""
LOAD_UI_TREE_DONE = 'load-ui-tree-done'

DRAG_AND_DROP = 'drag-and-drop'
DRAG_AND_DROP_DIRECT = 'drag-and-drop-direct'

TREE_SELECTION_CHANGED = 'tree-selection-changed'

# All components should listen for this
TOGGLE_UI_ENABLEMENT = 'toggle-ui-enablement'

PAUSE_OP_EXECUTION = 'pause-op-execution'
RESUME_OP_EXECUTION = 'resume-op-execution'
OP_EXECUTION_PLAY_STATE_CHANGED = 'op-execution-play-state-changed'

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
ID_GDRIVE_POLLING_THREAD = 'gdrive_polling_thread'

logger = logging.getLogger(__name__)

