from enum import IntEnum

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef', 'vob')

READ_CHUNK_SIZE = 1024 * 1024

CACHE_LOAD_TIMEOUT_SEC = 30

OBJ_TYPE_LOCAL_DISK = 1
OBJ_TYPE_GDRIVE = 2
OBJ_TYPE_DISPLAY_ONLY = 3

MAIN_REGISTRY_FILE_NAME = 'registry.db'
ROOT = '/'
GDRIVE_PATH_PREFIX = 'gdrive:/'

GDRIVE_CLIENT_REQUEST_MAX_RETRIES = 10

ICON_GENERIC_FILE = 'file'
ICON_TRASHED_DIR = 'trash-dir'
ICON_TRASHED_FILE = 'trash-file'
ICON_GENERIC_DIR = 'folder'

# GDrive
NOT_TRASHED = 0
EXPLICITLY_TRASHED = 1
IMPLICITLY_TRASHED = 2

TRASHED_STATUS = ['No', 'UserTrashed', 'Trashed']


class TreeDisplayMode(IntEnum):
    ONE_TREE_ALL_ITEMS = 1
    CHANGES_ONE_TREE_PER_CATEGORY = 2


