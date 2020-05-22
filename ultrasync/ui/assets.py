import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GdkPixbuf

from constants import ICON_ADD_DIR, ICON_GDRIVE, ICON_GENERIC_DIR, ICON_GENERIC_FILE, ICON_LOCAL_DISK, ICON_TRASHED_DIR, ICON_TRASHED_FILE

from model.fmeta import Category

from file_util import get_resource_path

ALERT_ICON_PATH = get_resource_path("resources/dialog-error-icon-24px.png")
GDRIVE_ICON_PATH = get_resource_path("resources/google-drive-logo-48px-scaled.png")
LOCAL_DISK_PATH = get_resource_path("resources/Filesystems-hd-linux-icon-48px.png")
CHOOSE_ROOT_ICON_PATH = LOCAL_DISK_PATH  # get_resource_path("resources/Folder-tree-flat-40px.png")
WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")
REFRESH_ICON_PATH = get_resource_path('resources/Refresh-icon-48px.png')


def _build_icons(icon_size):
    icons = dict()
    icons[ICON_GDRIVE] = GdkPixbuf.Pixbuf.new_from_file(GDRIVE_ICON_PATH)
    icons[ICON_LOCAL_DISK] = GdkPixbuf.Pixbuf.new_from_file(CHOOSE_ROOT_ICON_PATH)
    icons[ICON_GENERIC_FILE] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[ICON_GENERIC_DIR] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Folder-icon-{icon_size}px.png'))
    icons[ICON_ADD_DIR] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Folder_add_{icon_size}px.png'))
    icons[ICON_TRASHED_DIR] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/recycle-bag-{icon_size}px.png'))
    icons[ICON_TRASHED_FILE] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/icons8-paper-waste-{icon_size}px.png'))
    icons[Category.Nada.name] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[Category.Added.name] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-Add-icon-{icon_size}px.png'))
    icons[Category.Deleted.name] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-Delete-icon-{icon_size}px.png'))
    icons[Category.Moved.name] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[Category.Updated.name] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[Category.Ignored.name] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    return icons


class Assets:
    def __init__(self, config):
        self.config = config
        icon_size = config.get('display.diff_tree.icon_size')
        self._icons = _build_icons(icon_size)

    def get_icon(self, icon_name):
        return self._icons.get(icon_name, None)
