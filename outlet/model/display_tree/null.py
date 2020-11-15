from typing import Iterable, List, Optional

from model.node.node import Node
from model.display_tree.display_tree import DisplayTree


# CLASS NullDisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
from ui.tree.filter_criteria import FilterCriteria


class NullDisplayTree(DisplayTree):
    """A DisplayTree which has no nodes and does nothing. Useful for representing a tree whose root does not exist."""
    def __init__(self, app, tree_id, root_identifier):
        super().__init__(app, tree_id, root_identifier)

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return []

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return []

    def get_node_list_for_path_list(self, path_list: List[str]) -> List[Node]:
        return []

    def get_summary(self):
        # Should not return None
        return 'Tree does not exist'

    def refresh_stats(self, tree_id: str):
        pass
