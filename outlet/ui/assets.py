import os
import logging
from typing import Dict, List

from constants import BADGE_ICON_BASE_DIR, BASE_ICON_BASE_DIR, BTN_GDRIVE, BTN_LOCAL_DISK_LINUX, COMPOSITE_ICON_BASE_DIR, ICON_ALERT, ICON_DIR_CP_DST, \
    ICON_DIR_CP_SRC, ICON_DIR_MK, \
    ICON_DIR_MV_DST, \
    ICON_DIR_MV_SRC, ICON_DIR_RM, \
    ICON_DIR_TRASHED, ICON_DIR_UP_DST, ICON_DIR_UP_SRC, ICON_FILE_CP_DST, ICON_FILE_CP_SRC, ICON_FILE_MV_DST, \
    ICON_FILE_MV_SRC, ICON_FILE_RM, ICON_FILE_TRASHED, ICON_FILE_UP_DST, ICON_FILE_UP_SRC, ICON_FOLDER_TREE, ICON_GDRIVE, ICON_GENERIC_DIR, \
    ICON_GENERIC_FILE, ICON_IS_NOT_SHARED, ICON_IS_NOT_TRASHED, ICON_LOCAL_DISK_LINUX, ICON_MATCH_CASE, ICON_PAUSE, ICON_PLAY, ICON_REFRESH, \
    ICON_IS_SHARED, \
    ICON_IS_TRASHED, ICON_WINDOW
from model.node_identifier import ensure_int

from util.file_util import get_resource_path

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GdkPixbuf
from PIL import Image

logger = logging.getLogger(__name__)

REBUILD_IMAGES = True
VALID_ICON_SIZES = [16, 24, 32, 48, 64, 128, 256, 512, 1024]


# CLASS SimpleIcon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class SimpleIcon:
    def __init__(self, name: str, path: str):
        self.name = name
        self.icon_path: str = get_resource_path(path)

    def load(self) -> GdkPixbuf.Pixbuf:
        return GdkPixbuf.Pixbuf.new_from_file(self.icon_path)


# CLASS CompositeIcon
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CompositeIcon(SimpleIcon):
    def __init__(self, name: str, base_path: str, badges: List[str] = None):
        self.base_path: str = get_resource_path(base_path)
        icon_path = os.path.join(COMPOSITE_ICON_BASE_DIR, f'{name}.png')
        super().__init__(name, icon_path)
        self.badges: List[str] = badges
        if not self.badges:
            self.badges = []

    def load(self):
        if REBUILD_IMAGES or not os.path.exists(self.icon_path):
            self._generate_composite_icon()

        return GdkPixbuf.Pixbuf.new_from_file(self.icon_path)

    def _generate_composite_icon(self):
        logger.debug(f'Generating composite icon: {self.icon_path}')

        if not os.path.exists(self.base_path):
            raise RuntimeError(f'File does not exist: "{self.base_path}"')

        img_composite: Image = Image.open(self.base_path)

        for badge_name in self.badges:
            badge_path = get_resource_path(os.path.join(BADGE_ICON_BASE_DIR, f'{badge_name}.png'))
            if not os.path.exists(badge_path):
                raise RuntimeError(f'File does not exist: "{badge_path}"')

            img_badge: Image = Image.open(badge_path)

            height_offset = img_composite.height - img_badge.height
            box = (0, height_offset)

            try:
                img_composite.paste(img_badge, box=box, mask=img_badge)
            except ValueError:
                logger.debug(f'Composite paste() failed - converting to RGBA and trying again: "{badge_path}"')
                img_badge = img_badge.convert("RGBA")
                img_composite.paste(img_badge, box=box, mask=img_badge)

        logger.debug(f'Saving composite image: "{self.icon_path}"')
        comp_dir = get_resource_path(COMPOSITE_ICON_BASE_DIR)
        if not os.path.exists(comp_dir):
            os.makedirs(name=comp_dir, exist_ok=True)

        img_composite.save(self.icon_path)


# Static methods
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

def _build_icons(icon_size: int, badge_size: int):
    tool_icon_size = icon_size
    logger.debug(f'ToolIconSize is {tool_icon_size}')
    file_base: str = f'{BASE_ICON_BASE_DIR}/File-{icon_size}.png'
    dir_base: str = f'{BASE_ICON_BASE_DIR}/Dir-{icon_size}.png'
    hdisk_base: str = f'{BASE_ICON_BASE_DIR}/HDisk-{icon_size}.png'

    icon_list = [
        # Misc UI
        SimpleIcon(name=ICON_PLAY, path=f'resources/play-button-white-32px.png'),
        SimpleIcon(name=ICON_PAUSE, path=f'resources/pause-button-white-32px.png'),
        SimpleIcon(name=ICON_ALERT, path=f'resources/Dialog-error-icon-24px.png'),
        SimpleIcon(name=ICON_WINDOW, path=f'resources/app_icon.png'),
        SimpleIcon(name=ICON_REFRESH, path=f'resources/Badge/Refresh-icon-48px.png'),
        SimpleIcon(name=ICON_FOLDER_TREE, path=f'resources/Toolbar/FolderTree-{tool_icon_size}px.png'),
        SimpleIcon(name=ICON_MATCH_CASE, path=f'resources/Toolbar/MatchCase-{tool_icon_size}px.png'),
        SimpleIcon(name=ICON_IS_SHARED, path=f'resources/Toolbar/Shared-{tool_icon_size}px.png'),
        CompositeIcon(name=ICON_IS_NOT_SHARED, base_path=f'resources/Toolbar/Shared-{tool_icon_size}px.png', badges=[f'Cancel-{badge_size}']),
        SimpleIcon(name=ICON_IS_TRASHED, path=f'resources/Toolbar/Trashed-{tool_icon_size}px.png'),
        CompositeIcon(name=ICON_IS_NOT_TRASHED, base_path=f'resources/Toolbar/Trashed-{tool_icon_size}px.png', badges=[f'Cancel-{badge_size}']),
        SimpleIcon(name=BTN_GDRIVE, path="resources/google-drive-logo-48px-scaled.png"),
        CompositeIcon(name=BTN_LOCAL_DISK_LINUX, base_path=f'{BASE_ICON_BASE_DIR}/HDisk-48.png', badges=[f'linux-outline-{badge_size}']),

        # File
        SimpleIcon(name=ICON_GENERIC_FILE, path=file_base),
        CompositeIcon(name=ICON_FILE_RM, base_path=file_base, badges=[f'RM-{badge_size}']),
        CompositeIcon(name=ICON_FILE_MV_SRC, base_path=file_base, badges=[f'MV-src-{badge_size}']),
        CompositeIcon(name=ICON_FILE_UP_SRC, base_path=file_base, badges=[f'UP-src-{badge_size}']),
        CompositeIcon(name=ICON_FILE_CP_SRC, base_path=file_base, badges=[f'CP-src-{badge_size}']),
        CompositeIcon(name=ICON_FILE_MV_DST, base_path=file_base, badges=[f'MV-dst-{badge_size}']),
        CompositeIcon(name=ICON_FILE_UP_DST, base_path=file_base, badges=[f'UP-dst-{badge_size}']),
        CompositeIcon(name=ICON_FILE_CP_DST, base_path=file_base, badges=[f'CP-dst-{badge_size}']),
        SimpleIcon(name=ICON_FILE_TRASHED, path=f'resources/icons8-paper-waste-{icon_size}px.png'),

        # Dir
        SimpleIcon(name=ICON_GENERIC_DIR, path=dir_base),
        CompositeIcon(name=ICON_DIR_MK, base_path=dir_base, badges=[f'MKDIR-{badge_size}']),
        CompositeIcon(name=ICON_DIR_RM, base_path=dir_base, badges=[f'RM-{badge_size}']),
        CompositeIcon(name=ICON_DIR_MV_SRC, base_path=file_base, badges=[f'MV-src-{badge_size}']),
        CompositeIcon(name=ICON_DIR_UP_SRC, base_path=file_base, badges=[f'UP-src-{badge_size}']),
        CompositeIcon(name=ICON_DIR_CP_SRC, base_path=file_base, badges=[f'CP-src-{badge_size}']),
        CompositeIcon(name=ICON_DIR_MV_DST, base_path=dir_base, badges=[f'MV-dst-{badge_size}']),
        CompositeIcon(name=ICON_DIR_UP_DST, base_path=dir_base, badges=[f'UP-dst-{badge_size}']),
        CompositeIcon(name=ICON_DIR_CP_DST, base_path=dir_base, badges=[f'CP-dst-{badge_size}']),
        SimpleIcon(name=ICON_DIR_TRASHED, path=f'resources/recycle-bag-{icon_size}px.png'),

        # Drive
        SimpleIcon(name=ICON_GDRIVE, path="resources/google-drive-logo-48px-scaled.png"),
        CompositeIcon(name=ICON_LOCAL_DISK_LINUX, base_path=hdisk_base, badges=[f'linux-outline-{badge_size}']),
    ]

    icon_dict = dict()
    for icon in icon_list:
        icon_dict[icon.name] = icon
    return icon_dict


def _load_icons(icon_dict):
    icons = dict()
    for icon in icon_dict.values():
        icons[icon.name] = icon.load()
    return icons


# CLASS Assets
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Assets:
    def __init__(self, config):
        self.config = config
        icon_size = ensure_int(config.get('display.diff_tree.icon_size'))
        badge_size = ensure_int(config.get('display.diff_tree.badge_size'))
        self.icon_dict: Dict[str, SimpleIcon] = _build_icons(icon_size, badge_size)
        self._icons: Dict[str, GdkPixbuf.Pixbuf] = _load_icons(self.icon_dict)

    def get_icon(self, icon_name: str):
        return self._icons.get(icon_name, None)

    def get_path(self, icon_name: str):
        icon = self.icon_dict.get(icon_name, None)
        if icon:
            return icon.icon_path
        return None

