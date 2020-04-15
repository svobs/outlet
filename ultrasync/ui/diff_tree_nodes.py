import humanfriendly
import logging
logger = logging.getLogger(__name__)


class DirNode:
    """
    Represents a directory (i.e. not an FMeta)
    """
    def __init__(self, file_path, category):
        self.file_path = file_path
        self.file_count = 0
        self.size_bytes = 0
        self.category = category

    def add_meta(self, fmeta):
        if fmeta.category != self.category:
            logger.error(f'BAD CATEGORY: expected={self.category} found={fmeta.category} path={fmeta.file_path}')
        assert fmeta.category == self.category
        self.file_count += 1
        self.size_bytes += fmeta.size_bytes

    @classmethod
    def is_dir(cls):
        return True

    def get_summary(self):
        size = humanfriendly.format_size(self.size_bytes)
        return f'{size} in {self.file_count} files'


class CategoryNode(DirNode):
    """
    Represents a category in the tree (however it can possibly be treated as the root dir)
    """
    def __init__(self, category):
        super().__init__('', category)
