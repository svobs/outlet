from enum import IntEnum

from index.uid.uid import UID

APP_NAME = 'Outlet'

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef', 'vob')

READ_CHUNK_SIZE = 1024 * 1024

LARGE_NUMBER_OF_CHILDREN = 20000

CACHE_LOAD_TIMEOUT_SEC = 3000

HOLDOFF_TIME_MS = 1000

OP_TREE_INDENT_STR = '-> '

TREE_TYPE_NA = 0
TREE_TYPE_LOCAL_DISK = 1
TREE_TYPE_GDRIVE = 2
TREE_TYPE_MIXED = 3

OBJ_TYPE_FILE = 'FILE'
OBJ_TYPE_DIR = 'DIR'

MAIN_REGISTRY_FILE_NAME = 'registry.db'
PENDING_CHANGES_FILE_NAME = 'pending-changes.db'
ROOT_PATH = '/'
GDRIVE_PATH_PREFIX = 'gdrive:/'

GDRIVE_CLIENT_REQUEST_MAX_RETRIES = 10

PROGRESS_BAR_SLEEP_TIME_SEC = 0.5
PROGRESS_BAR_PULSE_STEP = 0.5
PROGRESS_BAR_MAX_MSG_LENGTH = 80

ICON_GENERIC_FILE = 'file'
ICON_ADD_FILE = 'add-file'
ICON_MODIFY_FILE = 'mod-file'
ICON_TRASHED_DIR = 'trash-dir'
ICON_TRASHED_FILE = 'trash-file'
ICON_GENERIC_DIR = 'folder'
ICON_ADD_DIR = 'add-dir'
ICON_GDRIVE = 'gdrive'
ICON_LOCAL_DISK = 'local'

FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT = 100  # TODO: put in config

# GDrive
NOT_TRASHED = 0
EXPLICITLY_TRASHED = 1
IMPLICITLY_TRASHED = 2

GDRIVE_DOWNLOAD_TYPE_LOAD_ALL = 1

GDRIVE_DOWNLOAD_STATE_NOT_STARTED = 0
GDRIVE_DOWNLOAD_STATE_GETTING_DIRS = 1
GDRIVE_DOWNLOAD_STATE_GETTING_NON_DIRS = 2
GDRIVE_DOWNLOAD_STATE_READY_TO_COMPILE = 3
GDRIVE_DOWNLOAD_STATE_COMPLETE = 10

TRASHED_STATUS = ['No', 'UserTrashed', 'Trashed']

# TODO: put in config
ENABLE_UID_PERSISTENCE = True
UID_RESERVATION_BLOCK_SIZE = 10000
"""The number of sequential UIDs to reserve each time we persist to disk. Setting to a higher number will mean less disk access, but
the UID numbers will get larger faster if there are a lot of program restarts, which is somewhere between annoying and inconvenient
when debugging"""

ROOT_UID = UID(1)
NULL_UID = UID(0)


class TreeDisplayMode(IntEnum):
    ONE_TREE_ALL_ITEMS = 1
    CHANGES_ONE_TREE_PER_CATEGORY = 2

