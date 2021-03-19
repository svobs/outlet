import os
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from constants import BADGE_ICON_BASE_DIR, BASE_ICON_BASE_DIR, BTN_GDRIVE, BTN_LOCAL_DISK_LINUX, COMPOSITE_ICON_BASE_DIR, \
    ICON_ALERT, ICON_DIR_CP_DST, \
    ICON_DIR_CP_SRC, ICON_DIR_MK, ICON_DIR_MV_DST, ICON_DIR_MV_SRC, ICON_DIR_RM, \
    ICON_DIR_TRASHED, ICON_DIR_UP_DST, ICON_DIR_UP_SRC, ICON_FILE_CP_DST, ICON_FILE_CP_SRC, ICON_FILE_MV_DST, \
    ICON_FILE_MV_SRC, ICON_FILE_RM, ICON_FILE_TRASHED, ICON_FILE_UP_DST, ICON_FILE_UP_SRC, ICON_FOLDER_TREE, ICON_GDRIVE, ICON_GENERIC_DIR, \
    ICON_GENERIC_FILE, ICON_IS_NOT_SHARED, ICON_IS_NOT_TRASHED, ICON_LOCAL_DISK_LINUX, ICON_MATCH_CASE, ICON_PAUSE, ICON_PLAY, ICON_REFRESH, \
    ICON_IS_SHARED, \
    ICON_IS_TRASHED, ICON_WINDOW, IconId, REBUILD_IMAGES
from util.ensure import ensure_int

from util.file_util import get_resource_path

from PIL import Image

logger = logging.getLogger(__name__)


class SimpleIcon:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SimpleIcon
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, name: str, path: str):
        self.name = name
        self.icon_path: str = get_resource_path(path)

    def build(self):
        pass


class CompositeIcon(SimpleIcon):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CompositeIcon
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, name: str, base_path: str, badges: List[str] = None):
        self.base_path: str = get_resource_path(base_path)
        icon_path = os.path.join(COMPOSITE_ICON_BASE_DIR, f'{name}.png')
        super().__init__(name, icon_path)
        self.badges: List[str] = badges
        if not self.badges:
            self.badges = []

    def build(self):
        if REBUILD_IMAGES or not os.path.exists(self.icon_path):
            self._generate_composite_icon()

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


class IconStore(ABC):
    """▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS IconStore
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend):
        self.backend = backend
        tree_icon_size = ensure_int(backend.get_config('display.image.tree_icon_size'))
        toolbar_icon_size = ensure_int(backend.get_config('display.image.toolbar_icon_size'))
        badge_size = ensure_int(backend.get_config('display.image.badge_size'))
        self._icon_meta_dict: Dict[IconId, SimpleIcon] = IconStore._build_icon_meta(tree_icon_size, toolbar_icon_size, badge_size)
        self._icon_dict: Dict[IconId, object] = {}

    def get_icon(self, icon_id: IconId) -> Optional:
        return self._icon_dict.get(icon_id, None)

    def load_all_icons(self):
        for icon_id, icon in self._icon_meta_dict.items():
            icon.build()
            self._icon_dict[icon_id] = self.load_icon(icon_id, icon)

    def get_path(self, icon_id: IconId) -> Optional[str]:
        icon_meta: SimpleIcon = self._icon_meta_dict.get(icon_id, None)
        if icon_meta:
            return icon_meta.icon_path
        return None

    @abstractmethod
    def load_icon(self, icon_id: IconId, icon: SimpleIcon) -> object:
        pass

    @staticmethod
    def _build_icon_meta(tree_icon_size: int, toolbar_icon_size: int, badge_size: int) -> Dict[IconId, SimpleIcon]:
        logger.debug(f'ToolIconSize is {toolbar_icon_size}')
        file_base: str = f'{BASE_ICON_BASE_DIR}/File-{tree_icon_size}.png'
        dir_base: str = f'{BASE_ICON_BASE_DIR}/Dir-{tree_icon_size}.png'
        hdisk_base: str = f'{BASE_ICON_BASE_DIR}/HDisk-{tree_icon_size}.png'

        # IMPORTANT: make sure these are in the same order as the IconId values in constants! (too lazy to enter each individually right now)
        icon_meta_dict = {
            # File
            IconId.ICON_GENERIC_FILE: SimpleIcon(name=ICON_GENERIC_FILE, path=file_base),
            IconId.ICON_FILE_RM: CompositeIcon(name=ICON_FILE_RM, base_path=file_base, badges=[f'RM-{badge_size}']),
            IconId.ICON_FILE_MV_SRC: CompositeIcon(name=ICON_FILE_MV_SRC, base_path=file_base, badges=[f'MV-src-{badge_size}']),
            IconId.ICON_FILE_UP_SRC: CompositeIcon(name=ICON_FILE_UP_SRC, base_path=file_base, badges=[f'UP-src-{badge_size}']),
            IconId.ICON_FILE_CP_SRC: CompositeIcon(name=ICON_FILE_CP_SRC, base_path=file_base, badges=[f'CP-src-{badge_size}']),
            IconId.ICON_FILE_MV_DST: CompositeIcon(name=ICON_FILE_MV_DST, base_path=file_base, badges=[f'MV-dst-{badge_size}']),
            IconId.ICON_FILE_UP_DST: CompositeIcon(name=ICON_FILE_UP_DST, base_path=file_base, badges=[f'UP-dst-{badge_size}']),
            IconId.ICON_FILE_CP_DST: CompositeIcon(name=ICON_FILE_CP_DST, base_path=file_base, badges=[f'CP-dst-{badge_size}']),
            IconId.ICON_FILE_TRASHED: SimpleIcon(name=ICON_FILE_TRASHED, path=f'resources/icons8-paper-waste-{tree_icon_size}px.png'),

            # Dir
            IconId.ICON_GENERIC_DIR: SimpleIcon(name=ICON_GENERIC_DIR, path=dir_base),
            IconId.ICON_DIR_MK: CompositeIcon(name=ICON_DIR_MK, base_path=dir_base, badges=[f'MKDIR-{badge_size}']),
            IconId.ICON_DIR_RM: CompositeIcon(name=ICON_DIR_RM, base_path=dir_base, badges=[f'RM-{badge_size}']),
            IconId.ICON_DIR_MV_SRC: CompositeIcon(name=ICON_DIR_MV_SRC, base_path=file_base, badges=[f'MV-src-{badge_size}']),
            IconId.ICON_DIR_UP_SRC: CompositeIcon(name=ICON_DIR_UP_SRC, base_path=file_base, badges=[f'UP-src-{badge_size}']),
            IconId.ICON_DIR_CP_SRC: CompositeIcon(name=ICON_DIR_CP_SRC, base_path=file_base, badges=[f'CP-src-{badge_size}']),
            IconId.ICON_DIR_MV_DST: CompositeIcon(name=ICON_DIR_MV_DST, base_path=dir_base, badges=[f'MV-dst-{badge_size}']),
            IconId.ICON_DIR_UP_DST: CompositeIcon(name=ICON_DIR_UP_DST, base_path=dir_base, badges=[f'UP-dst-{badge_size}']),
            IconId.ICON_DIR_CP_DST: CompositeIcon(name=ICON_DIR_CP_DST, base_path=dir_base, badges=[f'CP-dst-{badge_size}']),
            IconId.ICON_DIR_TRASHED: SimpleIcon(name=ICON_DIR_TRASHED, path=f'resources/recycle-bag-{tree_icon_size}px.png'),

            # Misc UI
            IconId.ICON_ALERT: SimpleIcon(name=ICON_ALERT, path=f'resources/Dialog-error-icon-24px.png'),
            IconId.ICON_WINDOW: SimpleIcon(name=ICON_WINDOW, path=f'resources/app_icon.png'),
            IconId.ICON_REFRESH: SimpleIcon(name=ICON_REFRESH, path=f'resources/Badge/Refresh-icon-48px.png'),
            IconId.ICON_PLAY: SimpleIcon(name=ICON_PLAY, path=f'resources/play-button-white-32px.png'),
            IconId.ICON_PAUSE: SimpleIcon(name=ICON_PAUSE, path=f'resources/pause-button-white-32px.png'),
            IconId.ICON_FOLDER_TREE: SimpleIcon(name=ICON_FOLDER_TREE, path=f'resources/Toolbar/FolderTree-{toolbar_icon_size}px.png'),
            IconId.ICON_MATCH_CASE: SimpleIcon(name=ICON_MATCH_CASE, path=f'resources/Toolbar/MatchCase-{toolbar_icon_size}px.png'),
            IconId.ICON_IS_SHARED: SimpleIcon(name=ICON_IS_SHARED, path=f'resources/Toolbar/Shared-{toolbar_icon_size}px.png'),
            IconId.ICON_IS_NOT_SHARED: CompositeIcon(name=ICON_IS_NOT_SHARED, base_path=f'resources/Toolbar/Shared-{toolbar_icon_size}px.png',
                                                     badges=[f'Cancel-{badge_size}']),
            IconId.ICON_IS_TRASHED: SimpleIcon(name=ICON_IS_TRASHED, path=f'resources/Toolbar/Trashed-{toolbar_icon_size}px.png'),
            IconId.ICON_IS_NOT_TRASHED: CompositeIcon(name=ICON_IS_NOT_TRASHED, base_path=f'resources/Toolbar/Trashed-{toolbar_icon_size}px.png',
                                                      badges=[f'Cancel-{badge_size}']),

            # Drive
            IconId.ICON_GDRIVE: SimpleIcon(name=ICON_GDRIVE, path="resources/google-drive-logo-48px-scaled.png"),
            IconId.ICON_LOCAL_DISK_LINUX: CompositeIcon(name=ICON_LOCAL_DISK_LINUX, base_path=hdisk_base, badges=[f'linux-outline-{badge_size}']),

            IconId.BTN_GDRIVE: SimpleIcon(name=BTN_GDRIVE, path="resources/google-drive-logo-48px-scaled.png"),
            IconId.BTN_LOCAL_DISK_LINUX: CompositeIcon(name=BTN_LOCAL_DISK_LINUX, base_path=f'{BASE_ICON_BASE_DIR}/HDisk-48.png',
                                                       badges=[f'linux-outline-{badge_size}']),
        }

        return icon_meta_dict


class IconStorePy(IconStore):
    """▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS IconStorePy

    An in-memory image cache which uses Python's cross-platform Pillow library's data struct for images
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def load_icon(self, icon_id: IconId, icon: SimpleIcon) -> object:
        return Image.open(icon.icon_path)
