import file_util
from fmeta.fmeta import Category
import gi
from file_util import get_resource_path
gi.require_version("Gtk", "3.0")
from gi.repository import GdkPixbuf


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
