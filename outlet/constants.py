from enum import IntEnum
from model.uid import UID

SUPER_DEBUG = True

FIND_DUPLICATE_GDRIVE_NODE_NAMES = False
COUNT_MULTIPLE_GDRIVE_PARENTS = False

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef', 'vob')

CACHE_LOAD_TIMEOUT_SEC = 3000

GRPC_SERVER_MAX_WORKER_THREADS = 5

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
GDRIVE_PATH_PREFIX = 'gdrive:/'

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

# --- FE + BE SHARED ---

ROOT_PATH = '/'

LOOPBACK_ADDRESS = '127.0.0.1'

ZEROCONF_SERVICE_NAME = 'OutletService'
ZEROCONF_SERVICE_VERSION = '1.0.0'
ZEROCONF_SERVICE_TYPE = '_outlet._tcp.local.'


TRASHED_STATUS_STR = ['No', 'UserTrashed', 'Trashed', 'Deleted']


# Trashed state
class TrashStatus(IntEnum):
    NOT_TRASHED = 0
    EXPLICITLY_TRASHED = 1
    IMPLICITLY_TRASHED = 2
    DELETED = 3

    def not_trashed(self) -> bool:
        return self == TrashStatus.NOT_TRASHED


# TODO: convert to enum
TREE_TYPE_NA = 0
TREE_TYPE_MIXED = 1
TREE_TYPE_LOCAL_DISK = 2
TREE_TYPE_GDRIVE = 3

TREE_TYPE_DISPLAY = {TREE_TYPE_NA: '✪', TREE_TYPE_LOCAL_DISK: 'L', TREE_TYPE_GDRIVE: 'G', TREE_TYPE_MIXED: 'M'}

# UID reserved values:
NULL_UID = UID(TREE_TYPE_NA)
SUPER_ROOT_UID = UID(TREE_TYPE_MIXED)
LOCAL_ROOT_UID = UID(TREE_TYPE_LOCAL_DISK)
GDRIVE_ROOT_UID = UID(TREE_TYPE_GDRIVE)
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

LARGE_NUMBER_OF_CHILDREN = 10000

# File icon names:
ICON_GENERIC_FILE = 'backend/store/local'
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
ICON_GDRIVE = 'gdrive'
ICON_LOCAL_DISK_LINUX = 'localdisk-linux'

BTN_GDRIVE = 'gdrive-btn'
BTN_LOCAL_DISK_LINUX = 'localdisk-linux-btn'


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

    ICON_GDRIVE = 31
    ICON_LOCAL_DISK_LINUX = 32

    BTN_GDRIVE = 33
    BTN_LOCAL_DISK_LINUX = 34


PROGRESS_BAR_SLEEP_TIME_SEC = 0.5
PROGRESS_BAR_PULSE_STEP = 0.5
PROGRESS_BAR_MAX_MSG_LENGTH = 80

FILTER_APPLY_DELAY_MS = 200
STATS_REFRESH_HOLDOFF_TIME_MS = 1000
