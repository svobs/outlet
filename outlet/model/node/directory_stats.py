from constants import TrashStatus
from typing import Optional


class DirectoryStats:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DirectoryStats

    Represents a generic directory (i.e. not an LocalFileNode or domain object) which contains metadeta about its
    enclosed descendants.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        self.file_count: int = 0
        self.trashed_file_count: int = 0
        self.dir_count: int = 0
        self.trashed_dir_count: int = 0
        self.trashed_bytes: int = 0
        self._size_bytes: int = 0
        """Set this to None to signify that stats are not yet calculated"""

    def update_from(self, other):
        if not isinstance(other, DirectoryStats):
            raise RuntimeError(f'Bad: {other} (we are: {self})')
        self.file_count = other.file_count
        self.trashed_file_count = other.trashed_file_count
        self.trashed_dir_count = other.trashed_dir_count
        self.dir_count = other.dir_count
        self.trashed_bytes = other.trashed_bytes
        self._size_bytes = other.get_size_bytes()

    def set_stats_for_no_children(self):
        self._size_bytes = 0
        self.file_count = 0
        self.dir_count = 0

    def add_dir_stats(self, child_dir_stats, child_is_trashed):
        # Child is DIR
        assert isinstance(child_dir_stats, DirectoryStats)
        self._size_bytes += child_dir_stats._size_bytes
        self.dir_count += child_dir_stats.dir_count
        self.file_count += child_dir_stats.file_count

        self.trashed_dir_count += child_dir_stats.trashed_dir_count
        self.trashed_file_count += child_dir_stats.file_count + child_dir_stats.trashed_file_count
        self.trashed_bytes += child_dir_stats.trashed_bytes

        if child_is_trashed:
            self.trashed_dir_count += 1
        else:
            self.dir_count += 1

    def add_file_node(self, child_node):
        if child_node.get_trashed_status() == TrashStatus.NOT_TRASHED:
            self.file_count += 1
            if child_node.get_size_bytes():
                self._size_bytes += child_node.get_size_bytes()
        else:
            self.trashed_file_count += 1
            if child_node.get_size_bytes():
                self.trashed_bytes += child_node.get_size_bytes()

    def get_etc(self) -> str:
        files = self.file_count + self.trashed_file_count
        if files == 1:
            multi = ''
        else:
            multi = 's'
        files_str = f'{files:n} file{multi}'

        folders = self.trashed_dir_count + self.dir_count
        if folders:
            if folders == 1:
                multi = ''
            else:
                multi = 's'
            folders_str = f', {folders:n} folder{multi}'
        else:
            folders_str = ''

        return f'{files_str}{folders_str}'

    def get_size_bytes(self) -> Optional[int]:
        return self._size_bytes

    def set_size_bytes(self, size_bytes: int):
        self._size_bytes = size_bytes
