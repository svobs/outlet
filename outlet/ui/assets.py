from typing import List

from model.op import OpType

from constants import ICON_DIR_CP_DST, ICON_DIR_CP_SRC, ICON_DIR_MK, ICON_DIR_MV_DST, ICON_DIR_MV_SRC, ICON_DIR_RM, ICON_DIR_TRASHED, ICON_DIR_UP_DST, \
    ICON_DIR_UP_SRC, \
    ICON_FILE_CP_DST, \
    ICON_FILE_CP_SRC, \
    ICON_FILE_MV_DST, \
    ICON_FILE_MV_SRC, \
    ICON_FILE_RM, \
    ICON_FILE_TRASHED, \
    ICON_FILE_UP_DST, \
    ICON_FILE_UP_SRC, \
    ICON_GDRIVE, \
    ICON_GENERIC_DIR, \
    ICON_GENERIC_FILE, \
    ICON_LOCAL_DISK

from util.file_util import get_resource_path

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GdkPixbuf

ALERT_ICON_PATH = get_resource_path("resources/Dialog-error-icon-24px.png")
CHOOSE_ROOT_ICON_PATH = get_resource_path("resources/Filesystems-hd-linux-icon-48px.png")
WINDOW_ICON_PATH = get_resource_path("resources/app_icon.png")
REFRESH_ICON_PATH = get_resource_path('resources/Badge/Refresh-icon-48px.png')
GDRIVE_ICON_PATH = get_resource_path("resources/google-drive-logo-48px-scaled.png")


class SimpleIcon:
    def __init__(self, name: str, path: str):
        self.name = name
        self.icon_path: str = get_resource_path(path)

    def load(self):
        return GdkPixbuf.Pixbuf.new_from_file(self.icon_path)


class CompositeIcon(SimpleIcon):
    def __init__(self, name: str, base_path: str, decorators: List[str] = None):
        super().__init__(name, base_path)
        self.decorators: List[str] = decorators
        if not self.decorators:
            self.decorators = []


def _build_icons(icon_size):
    icon_list = [
        SimpleIcon(name=ICON_GENERIC_FILE, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_RM, path=f'resources/Document-Delete-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_MV_SRC, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_UP_SRC, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_CP_SRC, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_MV_DST, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_UP_DST, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_CP_DST, path=f'resources/Node/Document-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_FILE_TRASHED, path=f'resources/icons8-paper-waste-{icon_size}px.png'),

        SimpleIcon(name=ICON_GENERIC_DIR, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_MK, path=f'resources/Folder_add_{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_RM, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_MV_SRC, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_UP_SRC, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_CP_SRC, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_MV_DST, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_UP_DST, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_CP_DST, path=f'resources/Node/Folder-icon-{icon_size}px.png'),
        SimpleIcon(name=ICON_DIR_TRASHED, path=f'resources/recycle-bag-{icon_size}px.png'),

        SimpleIcon(name=ICON_GDRIVE, path="resources/google-drive-logo-48px-scaled.png"),
        SimpleIcon(name=ICON_LOCAL_DISK, path="resources/Filesystems-hd-linux-icon-48px.png"),
    ]

    icons = dict()
    for icon in icon_list:
        icons[icon.name] = icon.load()
    return icons


class Assets:
    def __init__(self, config):
        self.config = config
        icon_size = config.get('display.diff_tree.icon_size')
        self._icons = _build_icons(icon_size)

    def get_icon(self, icon_name):
        return self._icons.get(icon_name, None)
