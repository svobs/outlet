import gi

from fmeta.fmeta import Category

gi.require_version("Gtk", "3.0")
from file_util import get_resource_path
from gi.repository import GdkPixbuf

ALERT_ICON_PATH = get_resource_path("resources/dialog-error-icon-24px.png")
CHOOSE_ROOT_ICON_PATH = get_resource_path("resources/Folder-tree-flat-40px.png")
GDRIVE_ICON_PATH = get_resource_path("resources/google-drive-logo-40px.png")
WINDOW_ICON_PATH = get_resource_path("resources/fslint_icon.png")

ICON_GENERIC_FILE = 'file'
ICON_TRASHED_DIR = 'trash-dir'
ICON_TRASHED_FILE = 'trash-file'
ICON_GENERIC_DIR = 'folder'


def _build_icons(icon_size):
    icons = dict()
    icons[ICON_GENERIC_FILE] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Document-icon-{icon_size}px.png'))
    icons[ICON_GENERIC_DIR] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/Folder-icon-{icon_size}px.png'))
    icons[ICON_TRASHED_DIR] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/recycle-bag-{icon_size}px.png'))
    icons[ICON_TRASHED_FILE] = GdkPixbuf.Pixbuf.new_from_file(get_resource_path(f'resources/icons8-paper-waste-{icon_size}px.png'))
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
        return self._icons[icon_name]


_assets = None


def init(config):
    global _assets
    _assets = Assets(config)


def get_icon(icon_name):
    if not icon_name:
        return None
    return _assets.get_icon(icon_name)
