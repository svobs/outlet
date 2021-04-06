from enum import IntEnum
from model.uid import UID

SUPER_DEBUG = True

FIND_DUPLICATE_GDRIVE_NODE_NAMES = False
COUNT_MULTIPLE_GDRIVE_PARENTS = False

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef', 'vob')

CACHE_LOAD_TIMEOUT_SEC = 3000

DEFAULT_MAIN_WIN_X = 50
DEFAULT_MAIN_WIN_Y = 50
DEFAULT_MAIN_WIN_WIDTH = 1200
DEFAULT_MAIN_WIN_HEIGHT = 500

# IMPORTANT: Each client will require 2 worker threads: 1 is for receiving signals asynchronously and is always open;
# and 1 is for all other requests to the backend.
GRPC_SERVER_MAX_WORKER_THREADS = 4

TASK_RUNNER_MAX_WORKERS = 1

READ_CHUNK_SIZE = 1024 * 1024

CACHE_WRITE_HOLDOFF_TIME_MS = 500

OP_TREE_INDENT_STR = '-> '

OBJ_TYPE_FILE = 'FILE'
OBJ_TYPE_DIR = 'DIR'

INDEX_FILE_SUFFIX = 'db'
MAIN_REGISTRY_FILE_NAME = f'registry.{INDEX_FILE_SUFFIX}'
GDRIVE_INDEX_FILE_NAME = f'GD.{INDEX_FILE_SUFFIX}'
OPS_FILE_NAME = f'ops.{INDEX_FILE_SUFFIX}'
UID_PATH_FILE_NAME = f'uid_path.{INDEX_FILE_SUFFIX}'

BASE_ICON_BASE_DIR = 'resources/Base'
COMPOSITE_ICON_BASE_DIR = 'resources/Composite'
BADGE_ICON_BASE_DIR = 'resources/Badge'

PROJECT_DIR = '.'
CONFIG_DIR = f'{PROJECT_DIR}/config'
DEFAULT_CONFIG_PATH = f'{CONFIG_DIR}/outlet-default.cfg'

CFG_ENABLE_LOAD_FROM_DISK = 'cache.enable_cache_load'

FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT = 100

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

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'
OPEN = 1
SHOW = 2

CONFIG_DELIMITER = ','

# --- FE + BE SHARED ---

ROOT_PATH = '/'
GDRIVE_PATH_PREFIX = 'gdrive:/'

LOOPBACK_ADDRESS = '127.0.0.1'

ZEROCONF_SERVICE_NAME = 'OutletService'
ZEROCONF_SERVICE_VERSION = '1.0.0'
ZEROCONF_SERVICE_TYPE = '_outlet._tcp.local.'


TRASHED_STATUS_STR = ['No', 'UserTrashed', 'Trashed', 'Deleted']

REBUILD_IMAGES = True
VALID_ICON_SIZES = [16, 24, 32, 48, 64, 128, 256, 512, 1024]


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


TREE_TYPE_DISPLAY = {TreeType.NA: '✪', TreeType.LOCAL_DISK: 'L', TreeType.GDRIVE: 'G', TreeType.MIXED: 'M'}

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

MIN_FREE_UID = 100


class TreeDisplayMode(IntEnum):
    ONE_TREE_ALL_ITEMS = 1
    CHANGES_ONE_TREE_PER_CATEGORY = 2


# --- FRONT END ONLY ---

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


PROGRESS_BAR_SLEEP_TIME_SEC = 0.5
PROGRESS_BAR_PULSE_STEP = 0.5
PROGRESS_BAR_MAX_MSG_LENGTH = 80

FILTER_APPLY_DELAY_MS = 200
STATS_REFRESH_HOLDOFF_TIME_MS = 1000
WIN_SIZE_STORE_DELAY_MS = 1000
