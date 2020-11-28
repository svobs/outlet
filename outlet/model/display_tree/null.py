from typing import Iterable, Optional

from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from ui.tree.filter_criteria import FilterCriteria


# CLASS NullDisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class NullDisplayTree(DisplayTree):
    """A DisplayTree which has no nodes and does nothing. Useful for representing a tree whose root does not exist."""
    def __init__(self, backend, state):
        super().__init__(backend, state)
        assert not state.root_exists, f'For state; {state}'

    def get_children_for_root(self, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return []

    def get_children(self, parent: Node, filter_criteria: FilterCriteria = None) -> Iterable[Node]:
        return []
