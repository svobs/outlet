from enum import IntEnum
from model.uid import UID
import platform

_system = platform.system().lower()
IS_WINDOWS = _system == 'windows'
IS_LINUX = _system == 'linux'
IS_MACOS = _system == 'darwin'

MACOS_SETFILE_DATETIME_FMT = '%m/%d/%Y %H:%M:%S'

GDRIVE_FIX_ORPHANS_ON_LOAD = True
GDRIVE_CHECK_FOR_BROKEN_NODES = True
GDRIVE_FIND_DUPLICATE_NODE_NAMES = False
GDRIVE_COUNT_MULTIPLE_PARENTS = False
OP_GRAPH_VALIDATE_AFTER_BATCH_INSERT = True

# When parsing config file:
PROJECT_DIR_TOKEN = '$PROJECT_DIR'
LOGGING_CONSTANTS_FILE = 'logging_constants.py'
INIT_FILE = '__init__.py'
TEMPLATE = 'template'

GRPC_CHANGE_TREE_NO_OP = 9

MAX_FS_LINK_DEPTH = 10

CACHE_LOAD_TIMEOUT_SEC = 3000

COMMAND_EXECUTION_TIMEOUT_SEC = 600

DEFAULT_MAIN_WIN_X = 50
DEFAULT_MAIN_WIN_Y = 50
DEFAULT_MAIN_WIN_WIDTH = 1200
DEFAULT_MAIN_WIN_HEIGHT = 500

CENTRAL_EXEC_THREAD_NAME = 'CentralExecutorThread'
OP_EXECUTION_THREAD_NAME = 'OpExecutionThread'

# IMPORTANT: Each client will require 2 worker threads: 1 is for receiving signals asynchronously and is always open;
# and 1 is for all other requests to the backend.
GRPC_SERVER_MAX_WORKER_THREADS = 4

# Add these two together to get the total possible number of concurrent workers:
TASK_RUNNER_MAX_COCURRENT_NON_USER_OP_TASKS = 1  # number of ops running with P5_USER_OP_EXECUTION priority
TASK_RUNNER_MAX_CONCURRENT_USER_OP_TASKS = 1  # number of tasks running with P5_USER_OP_EXECUTION priority

READ_CHUNK_SIZE = 1024 * 1024

CACHE_WRITE_HOLDOFF_TIME_MS = 500

TASK_EXEC_IMEOUT_SEC = 30

OP_TREE_INDENT_STR = '-> '

OBJ_TYPE_FILE = 'FILE'
OBJ_TYPE_DIR = 'DIR'

INDEX_FILE_SUFFIX = 'db'
MAIN_REGISTRY_FILE_NAME = f'registry.{INDEX_FILE_SUFFIX}'
GDRIVE_INDEX_FILE_NAME = f'GD.{INDEX_FILE_SUFFIX}'
OPS_FILE_NAME = f'ops.{INDEX_FILE_SUFFIX}'
UID_PATH_FILE_NAME = f'uid_path.{INDEX_FILE_SUFFIX}'
UID_GOOG_ID_FILE_NAME = f'uid_goog_id.{INDEX_FILE_SUFFIX}'

BASE_ICON_BASE_DIR = 'resources/Base'
COMPOSITE_ICON_BASE_DIR = 'resources/Composite'
BADGE_ICON_BASE_DIR = 'resources/Badge'

PROJECT_DIR = '.'
CONFIG_DIR = f'{PROJECT_DIR}/config'
CONFIG_PY_MODULE = 'config_py'
CONFIG_PY_DIR = f'{PROJECT_DIR}/{CONFIG_PY_MODULE}'
DEFAULT_CONFIG_PATH = f'{CONFIG_DIR}/outlet-default.cfg'

UI_STATE_CFG_SEGMENT = 'ui_state'

CFG_ENABLE_LOAD_FROM_DISK = 'cache.enable_cache_load'
CFG_ENABLE_OP_EXECUTION = 'ui_state.executor.enable_op_execution'

CFG_ENABLE_LAST_UID_PERSISTENCE = 'cache.enable_uid_lastval_persistence'
"""If true, read and write the last allocated UID value to 'ui_state.global.last_uid' so that duplicate UIDs aren't assigned across startups"""

CFG_UID_RESERVATION_BLOCK_SIZE = 'cache.uid_reservation_block_size'
"""The number of sequential UIDs to reserve each time we persist to disk. Setting to a higher number will mean less disk access, but
the UID numbers will get larger faster if there are a lot of program restarts, which is somewhere between annoying and inconvenient
when debugging"""

CFG_LAST_UID = f'{UI_STATE_CFG_SEGMENT}.global.last_uid'


FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT = 100

DISK_SCAN_MAX_ITEMS_PER_TASK = 1000
TASK_TIME_WARNING_THRESHOLD_SEC = 60

LARGE_FILE_SIZE_THRESHOLD_BYTES = 1000000000

# ---- Google Drive: ----

GDRIVE_CLIENT_SLEEP_ON_FAILURE_SEC = 3
GDRIVE_CLIENT_REQUEST_MAX_RETRIES = 10

GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD = 1
GDRIVE_DOWNLOAD_TYPE_CHANGES = 2

GDRIVE_DOWNLOAD_STATE_NOT_STARTED = 0
GDRIVE_DOWNLOAD_STATE_GETTING_DIRS = 1
GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS = 2
GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE = 3
GDRIVE_DOWNLOAD_STATE_COMPLETE = 10

# IMPORTANT: If modifying these scopes, delete the file token.pickle.
# GDRIVE_AUTH_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
GDRIVE_AUTH_SCOPES = ['https://www.googleapis.com/auth/drive']

MIME_TYPE_SHORTCUT = 'application/vnd.google-apps.shortcut'
MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'

QUERY_FOLDERS_ONLY = f"mimeType='{MIME_TYPE_FOLDER}'"
QUERY_NON_FOLDERS_ONLY = f"not {QUERY_FOLDERS_ONLY}"

# Web view link takes the form:
GDRIVE_WEB_VIEW_LINK = 'https://drive.google.com/file/d/{id}/view?usp=drivesdk'
GDRIVE_WEB_CONTENT_LINK = 'https://drive.google.com/uc?id={id}&export=download'

GDRIVE_FOLDER_FIELDS = 'id, name, mimeType, trashed, explicitlyTrashed, driveId, shared, owners, sharingUser, createdTime, modifiedTime'
GDRIVE_FILE_FIELDS = f'{GDRIVE_FOLDER_FIELDS}, version, createdTime, modifiedTime, owners, md5Checksum, size, headRevisionId, ' \
                     f'shortcutDetails, sharingUser'

GDRIVE_FOLDER_MIME_TYPE_UID = 1

GDRIVE_ME_USER_UID = 1

GDRIVE_MY_DRIVE_ROOT_GOOG_ID = 'root'

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'
OPEN = 1
SHOW = 2

CONFIG_DELIMITER = ','

# --- FE + BE SHARED ---

ROOT_PATH = '/'

LOOPBACK_ADDRESS = '127.0.0.1'

ZEROCONF_SERVICE_NAME = 'OutletService'
ZEROCONF_SERVICE_VERSION = '1.0.0'
ZEROCONF_SERVICE_TYPE = '_outlet._tcp.local.'


TRASHED_STATUS_STR = ['No', 'UserTrashed', 'Trashed', 'Deleted']

REBUILD_IMAGES = True
VALID_ICON_SIZES = [16, 24, 32, 48, 64, 128, 256, 512, 1024]

# Simple typedef
TreeID = str
GoogID = str


class DeleteBehavior(IntEnum):
    TO_CUSTOM_TRASH = 1
    TO_NATIVE_TRASH = 2
    DELETE_IMMEDIATELY = 3


# Trashed state
class TrashStatus(IntEnum):
    NOT_TRASHED = 0
    EXPLICITLY_TRASHED = 1
    IMPLICITLY_TRASHED = 2
    DELETED = 3

    def not_trashed(self) -> bool:
        return self == TrashStatus.NOT_TRASHED


class TreeType(IntEnum):
    NA = 0
    MIXED = 1
    LOCAL_DISK = 2
    GDRIVE = 3


class SPIDType(IntEnum):
    NOT_A_SPID = 0
    STANDARD = 1
    CHANGE_TREE = 2


class DragOperation(IntEnum):
    MOVE = 1
    COPY = 2
    LINK = 3
    DELETE = 4


class DirConflictPolicy(IntEnum):
    """
    For operations where the src dir and dst dir have same name but different content.
    This determines the operations which are created at the time of drop.
    """

    PROMPT = 1
    """Prompt user every time and display remaining options TODO: also show live preview"""

    SKIP = 2
    """Leave dst dir alone. Note: For MOVE operations, this will leave the src dir as-is"""

    REPLACE = 10
    """Essentially delete all children in dst dir and replace it with src dir contents"""

    RENAME = 20
    """AKA keep both"""

    MERGE = 30
    """Recursively dive into src & dst dirs and follow FileConflictPolicy rules for each file"""


class FileConflictPolicy(IntEnum):
    """
    For operations where the src file and dst file has same same name but different content.
    This determines the operations which are created at the time of drop.
    """

    PROMPT = 1
    """Prompt user every time and display remaining options
    TODO: also show preview of changes"""

    SKIP = 2
    """Leave the existing dst file. AKA REPLACE_NEVER"""

    REPLACE_ALWAYS = 10
    """Update/replace the dst file with the src file"""

    REPLACE_IF_OLDER_AND_DIFFERENT = 11
    """Update/replace the dst file with the src file IF the src file is different AND has a newer modify TS.
    Note that if src and dst are identical but have different timestamps, the dst file's timestamps will not be changed"""

    RENAME_ALWAYS = 20
    """Rename the target file so there is no collision (i.e. keep both files).
    This can result in files with duplicate content being created side by side."""

    RENAME_IF_OLDER_AND_DIFFERENT = 21
    """Rename the target file so there is no collision (i.e. keep both files) if the src file is has different content and has a later modify TS"""

    RENAME_IF_DIFFERENT = 22
    """Rename the target file so there is no collision (i.e. keep both files) if the src file is has different content"""


class SrcNodeMovePolicy(IntEnum):
    DELETE_SRC_ALWAYS = 1
    """Delete the src node EVEN IF it is skipped. NOTE: this will likely result in loss of data if user doesn't know what they are doing"""

    DELETE_SRC_IF_NOT_SKIPPED = 2
    """Delete the src node if a node with the same content is represented at the dst. This corresponds to the more intuitive sense of 
    "the move was successful".
    Do not delete if (A) the operation in question ended up following the SKIP policy, (B) it ended up following a policy with a conditional test
    which failed to be true. All other situations will result in delete.
    Examples:
    1. User drops with REPLACE_IF_NEWER_VER, and the dst node is found to be a newer version than the src node:
       -> Dst node is not copied. Src node is not deleted
    2. User drops with RENAME_IF_NEWER_VER, and the dst file is found to be same version as the src file:
       -> Dst node is not copied. Src node is deleted because it is same as dst
    3. User drops with SKIP, and dst file has the same name as the src file:
       -> Dst node is not copied. Src node is not deleted
   """


class ReplaceDirWithFilePolicy(IntEnum):
    PROMPT = 1
    """Prompt the user for what to do"""

    FAIL = 2
    """Disallow the containing operation and throw an error back to the user"""

    FOLLOW_FILE_POLICY_FOR_DIR = 3
    """Act like the dir is a file: go ahead and follow the FileConflictPolicy."""


# For batch or single op failures
class ErrorHandlingStrategy(IntEnum):
    PROMPT = 1
    CANCEL_BATCH = 2
    CANCEL_FAILED_OPS_AND_ALL_DESCENDANT_OPS = 3
    CANCEL_FAILED_OPS_ONLY = 4


DEFAULT_SRC_NODE_MOVE_POLICY = SrcNodeMovePolicy.DELETE_SRC_IF_NOT_SKIPPED
DEFAULT_REPLACE_DIR_WITH_FILE_POLICY = ReplaceDirWithFilePolicy.FAIL
DEFAULT_ERROR_HANDLING_STRATEGY = ErrorHandlingStrategy.PROMPT


# For tree context menus: see gRPC TreeContextMenuItem
class MenuItemType(IntEnum):
    NORMAL = 1
    SEPARATOR = 2
    DISABLED = 3
    ITALIC_DISABLED = 4


class ActionID(IntEnum):
    NO_ACTION = 1
    REFRESH = 2                  # FE only (should be BE though)
    EXPAND_ALL = 3               # FE only
    GO_INTO_DIR = 4              # FE only (should be BE though)
    SHOW_IN_FILE_EXPLORER = 5    # FE only
    OPEN_WITH_DEFAULT_APP = 6    # FE only
    DELETE_SINGLE_FILE = 7       # FE only (should be BE though)
    DELETE_SUBTREE = 8           # FE only (should be BE though)
    DELETE_SUBTREE_FOR_SINGLE_DEVICE = 9  # BE: requires: target_guid_list
    DOWNLOAD_FROM_GDRIVE = 10    # FE only (should be BE though)
    SET_ROWS_CHECKED = 11        # FE only
    SET_ROWS_UNCHECKED = 12      # FE only
    EXPAND_ROWS = 13             # BE -> FE
    COLLAPSE_ROWS = 14           # BE -> FE

    CALL_EXIFTOOL = 50           # FE only

    ACTIVATE = 100

    # 101+ are reserved for custom actions!


# Beyond this amount, an error msg will be displayed
MAX_ROWS_PER_ACTIVATION = 20

TREE_TYPE_DISPLAY = {TreeType.NA: '✪', TreeType.MIXED: 'M', TreeType.LOCAL_DISK: 'L', TreeType.GDRIVE: 'G'}

# UID reserved values:
NULL_UID = UID(TreeType.NA)
SUPER_ROOT_UID = UID(TreeType.MIXED)
LOCAL_ROOT_UID = UID(TreeType.LOCAL_DISK)
GDRIVE_ROOT_UID = UID(TreeType.GDRIVE)
ROOT_PATH_UID = LOCAL_ROOT_UID
assert NULL_UID == 0
assert SUPER_ROOT_UID == 1
assert LOCAL_ROOT_UID == 2
assert GDRIVE_ROOT_UID == 3

SUPER_ROOT_DEVICE_UID = SUPER_ROOT_UID

MIN_FREE_UID = 100


def is_root(node_uid: UID) -> bool:
    return SUPER_ROOT_UID <= node_uid <= GDRIVE_ROOT_UID


class TreeDisplayMode(IntEnum):
    ONE_TREE_ALL_ITEMS = 1
    CHANGES_ONE_TREE_PER_CATEGORY = 2


class TreeLoadState(IntEnum):
    UNKNOWN = 0  # should never be returned
    NOT_LOADED = 1
    LOAD_STARTED = 2
    COMPLETELY_LOADED = 10  # final state: all unfiltered + filtered nodes loaded


class EngineSummaryState(IntEnum):
    RED = 0
    YELLOW = 1
    GREEN = 2


# --- FE ONLY ---

APP_NAME = 'Outlet'

# Padding in pixels
H_PAD = 5
V_PAD = 5

MAX_NUMBER_DISPLAYABLE_CHILD_NODES = 10000

# File icon names:
ICON_GENERIC_FILE = 'backend/tree_store/local'
ICON_FILE_RM = 'file-rm'
ICON_FILE_MV_SRC = 'file-mv-src'
ICON_FILE_UP_SRC = 'file-up-src'
ICON_FILE_CP_SRC = 'file-cp-src'
ICON_FILE_MV_DST = 'file-mv-dst'
ICON_FILE_UP_DST = 'file-up-dst'
ICON_FILE_CP_DST = 'file-cp-dst'
ICON_FILE_TRASHED = 'file-trashed'
ICON_FILE_ERROR = 'file-error'
ICON_FILE_WARNING = 'file-warning'

# Dir icon names:
ICON_GENERIC_DIR = 'dir'
ICON_DIR_MK = 'dir-mk'
ICON_DIR_RM = 'dir-rm'
ICON_DIR_MV_SRC = 'dir-mv-src'
ICON_DIR_UP_SRC = 'dir-up-src'
ICON_DIR_CP_SRC = 'dir-cp-src'
ICON_DIR_MV_DST = 'dir-mv-dst'
ICON_DIR_UP_DST = 'dir-up-dst'
ICON_DIR_CP_DST = 'dir-cp-dst'
ICON_DIR_TRASHED = 'dir-trashed'
ICON_DIR_ERROR = 'dir-error'
ICON_DIR_WARNING = 'dir-warning'
ICON_DIR_PENDING_DOWNSTREAM_OP = 'dir-pending-downstream-op'

# Various icon names:
ICON_ALERT = 'alert'
ICON_WINDOW = 'win'
ICON_REFRESH = 'refresh'
ICON_FOLDER_TREE = 'folder-tree'
ICON_MATCH_CASE = 'match-case'
ICON_IS_SHARED = 'is-shared'
ICON_IS_NOT_SHARED = 'is-not-shared'
ICON_IS_TRASHED = 'is-trashed'
ICON_IS_NOT_TRASHED = 'is-not-trashed'
ICON_PLAY = 'play'
ICON_PAUSE = 'pause'

# Root icon names:
ICON_LOCAL_DISK_LINUX = 'localdisk-linux'
ICON_LOCAL_DISK_MACOS = 'localdisk-macos'
ICON_LOCAL_DISK_WINDOWS = 'localdisk-win'
ICON_GDRIVE = 'gdrive'

BTN_FOLDER_TREE = 'folder-tree-btn'
BTN_LOCAL_DISK_LINUX = 'localdisk-linux-btn'
BTN_LOCAL_DISK_MACOS = 'localdisk-macos-btn'
BTN_LOCAL_DISK_WINDOWS = 'localdisk-win-btn'
BTN_GDRIVE = 'gdrive-btn'

ICON_LOADING = 'loading'


class IconId(IntEnum):
    """Used for identifying icons in a more compact way, mainly for serialization for RPC"""
    NONE = 0

    ICON_GENERIC_FILE = 1
    ICON_FILE_RM = 2
    ICON_FILE_MV_SRC = 3
    ICON_FILE_UP_SRC = 4
    ICON_FILE_CP_SRC = 5
    ICON_FILE_MV_DST = 6
    ICON_FILE_UP_DST = 7
    ICON_FILE_CP_DST = 8
    ICON_FILE_TRASHED = 9

    ICON_GENERIC_DIR = 10
    ICON_DIR_MK = 11
    ICON_DIR_RM = 12
    ICON_DIR_MV_SRC = 13
    ICON_DIR_UP_SRC = 14
    ICON_DIR_CP_SRC = 15
    ICON_DIR_MV_DST = 16
    ICON_DIR_UP_DST = 17
    ICON_DIR_CP_DST = 18
    ICON_DIR_TRASHED = 19

    ICON_ALERT = 20
    ICON_WINDOW = 21
    ICON_REFRESH = 22
    ICON_PLAY = 23
    ICON_PAUSE = 24
    ICON_FOLDER_TREE = 25
    ICON_MATCH_CASE = 26
    ICON_IS_SHARED = 27
    ICON_IS_NOT_SHARED = 28
    ICON_IS_TRASHED = 29
    ICON_IS_NOT_TRASHED = 30

    ICON_LOCAL_DISK_LINUX = 31
    ICON_LOCAL_DISK_MACOS = 32
    ICON_LOCAL_DISK_WINDOWS = 33
    ICON_GDRIVE = 34

    BTN_FOLDER_TREE = 40
    BTN_LOCAL_DISK_LINUX = 41
    BTN_LOCAL_DISK_MACOS = 42
    BTN_LOCAL_DISK_WINDOWS = 43
    BTN_GDRIVE = 44

    ICON_LOADING = 50

    ICON_TO_ADD = 51
    ICON_TO_DELETE = 52
    ICON_TO_UPDATE = 53
    ICON_TO_MOVE = 54

    BADGE_RM = 100
    BADGE_MV_SRC = 101
    BADGE_MV_DST = 102
    BADGE_CP_SRC = 103
    BADGE_CP_DST = 104
    BADGE_UP_SRC = 105
    BADGE_UP_DST = 106
    BADGE_MKDIR = 107
    BADGE_TRASHED = 108

    BADGE_CANCEL = 109
    BADGE_REFRESH = 110
    BADGE_PENDING_DOWNSTREAM_OP = 111
    BADGE_ERROR = 112
    BADGE_WARNING = 113

    BADGE_LINUX = 120
    BADGE_MACOS = 121
    BADGE_WINDOWS = 122

    ICON_DIR_PENDING_DOWNSTREAM_OP = 130
    ICON_FILE_ERROR = 131
    ICON_DIR_ERROR = 132
    ICON_FILE_WARNING = 133
    ICON_DIR_WARNING = 134


PROGRESS_BAR_SLEEP_TIME_SEC = 0.5
PROGRESS_BAR_PULSE_STEP = 0.5
PROGRESS_BAR_MAX_MSG_LENGTH = 80

FILTER_APPLY_DELAY_MS = 200
STATS_REFRESH_HOLDOFF_TIME_MS = 500
ROWS_OF_INTEREST_SAVE_HOLDOFF_TIME_MS = 2000
WIN_SIZE_STORE_DELAY_MS = 1000
