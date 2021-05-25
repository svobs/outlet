from enum import IntEnum

# Note: this file cannot be named "signal.py" because it will result in a namespace conflict with an imported library


class Signal(IntEnum):
    # Tasks
    ENQUEUE_UI_TASK = 1
    START_CACHEMAN = 2
    START_OP_EXEC_THREAD = 3
    LOAD_REGISTRY_DONE = 4
    START_CACHEMAN_DONE = 5
    DIFF_TREES_DONE = 7
    DIFF_TREES_FAILED = 8
    DIFF_TREES_CANCELLED = 9
    SYNC_GDRIVE_CHANGES = 10
    DOWNLOAD_ALL_GDRIVE_META = 11
    COMMAND_COMPLETE = 12
    GENERATE_MERGE_TREE_DONE = 13
    GENERATE_MERGE_TREE_FAILED = 14

    COMPLETE_MERGE = 15
    """Request from FE to BE"""

    NODE_UPSERTED_IN_CACHE = 16
    """Internal to BE: should only be received by ActiveTreeManager"""
    NODE_REMOVED_IN_CACHE = 17
    """Internal to BE: should only be received by ActiveTreeManager"""
    # NODE_MOVED_IN_CACHE = 18
    """Internal to BE: should only be received by ActiveTreeManager"""

    # --- Tree actions: requests ---
    CALL_EXIFTOOL = 20
    CALL_EXIFTOOL_LIST = 21
    SHOW_IN_NAUTILUS = 22
    CALL_XDG_OPEN = 23
    EXPAND_AND_SELECT_NODE = 24
    EXPAND_ALL = 25
    DOWNLOAD_FROM_GDRIVE = 26
    DELETE_SINGLE_FILE = 27
    DELETE_SUBTREE = 28
    SET_ROWS_CHECKED = 29
    SET_ROWS_UNCHECKED = 30
    FILTER_UI_TREE = 33
    """Requests that the central cache update the stats for all nodes in the given subtree.
    When done, the central cache will send the signal REFRESH_SUBTREE_STATS_DONE to notify the tree that it can redraw the displayed nodes"""
    SHUTDOWN_APP = 34
    DEREGISTER_DISPLAY_TREE = 35

    # --- Tree actions: notifications ---
    LOAD_SUBTREE_STARTED = 40
    """Fired by the backend when it has begun to load a subtree from cache"""
    LOAD_SUBTREE_DONE = 41
    """Fired by the backend when it has finsished loading a subtree from cache"""
    NODE_EXPANSION_TOGGLED = 42
    NODE_EXPANSION_DONE = 43
    DISPLAY_TREE_CHANGED = 44
    GDRIVE_RELOADED = 45
    NODE_UPSERTED = 46
    """Sent from BE and received by FE"""
    NODE_REMOVED = 47
    """Sent from BE and received by FE"""

    EXIT_DIFF_MODE = 49
    """Sent from FE and received by BE"""

    ERROR_OCCURRED = 50
    REFRESH_SUBTREE_STATS_DONE = 51
    REFRESH_SUBTREE_STATS_COMPLETELY_DONE = 52
    DOWNLOAD_FROM_GDRIVE_DONE = 53
    """Indicates that the central cache has updated the stats for the subtree, and the subtree should redraw the nodes"""
    POPULATE_UI_TREE_DONE = 54
    """This is fired by the UI when it has finished populating the UI tree"""

    DEVICE_UPSERTED = 55
    """A Device was added or updated (includes the relevant Device in the msg)"""

    DRAG_AND_DROP = 60
    DRAG_AND_DROP_DIRECT = 61

    TREE_SELECTION_CHANGED = 70

    # All components should listen for this
    TOGGLE_UI_ENABLEMENT = 80

    PAUSE_OP_EXECUTION = 90
    RESUME_OP_EXECUTION = 91
    OP_EXECUTION_PLAY_STATE_CHANGED = 92

    # --- Progress bar ---
    START_PROGRESS_INDETERMINATE = 100
    START_PROGRESS = 101
    SET_PROGRESS_TEXT = 102
    PROGRESS_MADE = 103
    STOP_PROGRESS = 104

    # --- Status bar ---
    SET_STATUS = 105

    # gRPC, sent from server to client
    WELCOME = 200


# --- Sender identifiers ---
ID_MAIN_WINDOW = 'main_win'
ID_LEFT_TREE = 'left_tree'
ID_RIGHT_TREE = 'right_tree'
ID_LEFT_DIFF_TREE = f'{ID_LEFT_TREE}_diff'
ID_RIGHT_DIFF_TREE = f'{ID_RIGHT_TREE}_diff'
ID_MERGE_TREE = 'merge_tree'

ID_GDRIVE_DIR_SELECT = 'gdrive_dir_select'
ID_GLOBAL_CACHE = 'global_cache'
ID_COMMAND_EXECUTOR = 'command-executor'
ID_CENTRAL_EXEC = 'central-executor'
ID_GDRIVE_POLLING_THREAD = 'gdrive_polling_thread'
