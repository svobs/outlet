from enum import IntEnum
from model.uid import UID

SUPER_DEBUG = False

APP_NAME = 'Outlet'

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef', 'vob')

TASK_RUNNER_MAX_WORKERS = 1

READ_CHUNK_SIZE = 1024 * 1024

LARGE_NUMBER_OF_CHILDREN = 20000

CACHE_LOAD_TIMEOUT_SEC = 3000

HOLDOFF_TIME_MS = 1000

OP_TREE_INDENT_STR = '-> '

FIND_DUPLICATE_GDRIVE_NODE_NAMES = False
COUNT_MULTIPLE_GDRIVE_PARENTS = False

TREE_TYPE_NA = 0
TREE_TYPE_LOCAL_DISK = 1
TREE_TYPE_GDRIVE = 2
TREE_TYPE_MIXED = 3

OBJ_TYPE_FILE = 'FILE'
OBJ_TYPE_DIR = 'DIR'

MAIN_REGISTRY_FILE_NAME = 'registry.db'
OPS_FILE_NAME = 'ops.db'
ROOT_PATH = '/'
GDRIVE_PATH_PREFIX = 'gdrive:/'

GDRIVE_CLIENT_REQUEST_MAX_RETRIES = 10

PROGRESS_BAR_SLEEP_TIME_SEC = 0.5
PROGRESS_BAR_PULSE_STEP = 0.5
PROGRESS_BAR_MAX_MSG_LENGTH = 80

BASE_ICON_BASE_DIR = 'resources/Base'
COMPOSITE_ICON_BASE_DIR = 'resources/Composite'
BADGE_ICON_BASE_DIR = 'resources/Badge'

PROJECT_DIR = '.'
CONFIG_DIR = f'{PROJECT_DIR}/config'
DEFAULT_CONFIG_PATH = f'{CONFIG_DIR}/outlet-default.cfg'

# Various icon names:
ICON_ALERT = 'alert'
ICON_WINDOW = 'win'
ICON_REFRESH = 'refresh'

# File icon names:
ICON_GENERIC_FILE = 'store/local'
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

# Root icon names:
ICON_GDRIVE = 'gdrive'
ICON_LOCAL_DISK_LINUX = 'localdisk-linux'

BTN_GDRIVE = 'gdrive-btn'
BTN_LOCAL_DISK_LINUX = 'localdisk-linux-btn'

FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT = 100

# GDrive
GDRIVE_DOWNLOAD_TYPE_INITIAL_LOAD = 1
GDRIVE_DOWNLOAD_TYPE_CHANGES = 2

GDRIVE_DOWNLOAD_STATE_NOT_STARTED = 0
GDRIVE_DOWNLOAD_STATE_GETTING_DIRS = 1
GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS = 2
GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE = 3
GDRIVE_DOWNLOAD_STATE_COMPLETE = 10

TRASHED_STATUS_STR = ['No', 'UserTrashed', 'Trashed', 'Deleted']


# Trashed state
class TrashStatus(IntEnum):
    NOT_TRASHED = 0
    EXPLICITLY_TRASHED = 1
    IMPLICITLY_TRASHED = 2
    DELETED = 3

    def not_trashed(self) -> bool:
        return self == TrashStatus.NOT_TRASHED


# UID reserved values:
SUPER_ROOT_UID = UID(1)
LOCAL_ROOT_UID = UID(2)
GDRIVE_ROOT_UID = UID(3)
NULL_UID = UID(0)


class TreeDisplayMode(IntEnum):
    ONE_TREE_ALL_ITEMS = 1
    CHANGES_ONE_TREE_PER_CATEGORY = 2


# ---- Google Drive: ----
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
