from typing import Iterable, List, Optional

from model.display_node import DisplayNode
from model.node_identifier import NodeIdentifier
from model.subtree_snapshot import SubtreeSnapshot


class NullSubtree(SubtreeSnapshot):
    def __init__(self, root_identifier):
        super().__init__(root_identifier)

    @classmethod
    def create_identifier(cls, full_path, uid, category) -> NodeIdentifier:
        raise RuntimeError('Should not do this')

    def get_all(self) -> List[DisplayNode]:
        return []

    def get_children_for_root(self) -> Iterable[DisplayNode]:
        return []

    def get_children(self, node: DisplayNode) -> Iterable[DisplayNode]:
        return []

    def get_ignored_items(self):
        return []

    def get_parent_for_item(self, item) -> Optional[DisplayNode]:
        raise RuntimeError('Should not do this')

    def get_full_path_for_item(self, item) -> str:
        raise RuntimeError('Should not do this')

    def get_relative_path_for_item(self, item):
        raise RuntimeError('Should not do this')

    def get_for_path(self, path: str, include_ignored=False) -> List[DisplayNode]:
        return []

    def get_md5_dict(self):
        raise RuntimeError('Should not do this')

    def add_item(self, item):
        raise RuntimeError('Cannot add item to NullSubtree!')

    def get_summary(self):
        # Should not return None
        return 'Tree does not exist'
