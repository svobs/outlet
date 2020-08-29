from abc import ABC, abstractmethod
from collections import deque
import logging
from typing import Callable, Deque, Iterable, List, Optional

from pydispatch import dispatcher

from util.stopwatch_sec import Stopwatch
from model.node.display_node import DisplayNode
from ui import actions

logger = logging.getLogger(__name__)


# ABSTRACT CLASS DisplayTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DisplayTree(ABC):
    def __init__(self, root_node: DisplayNode):
        super().__init__()
        assert isinstance(root_node, DisplayNode)
        self.root_node: DisplayNode = root_node

        self._stats_loaded = False

    # From the root node_identifier
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @property
    def node_identifier(self):
        return self.root_node.node_identifier

    @property
    def tree_type(self) -> int:
        return self.root_node.node_identifier.tree_type

    @property
    def root_path(self):
        return self.root_node.node_identifier.full_path

    @property
    def root_uid(self):
        return self.uid

    @property
    def uid(self):
        return self.root_node.node_identifier.uid

    def in_this_subtree(self, path: str):
        if isinstance(path, list):
            for p in path:
                # i.e. any
                if p.startswith(self.root_path):
                    return True
            return False

        return path.startswith(self.root_path)

    # Getters & search
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_children_for_root(self) -> Iterable[DisplayNode]:
        pass

    @abstractmethod
    def get_children(self, parent: DisplayNode) -> Iterable[DisplayNode]:
        pass

    @abstractmethod
    def get_parent_for_item(self, item) -> Optional[DisplayNode]:
        pass

    @abstractmethod
    def get_full_path_for_item(self, item) -> str:
        pass

    @abstractmethod
    def get_relative_path_for_item(self, item):
        pass

    @abstractmethod
    def get_for_path(self, path: str, include_ignored=False) -> List[DisplayNode]:
        pass

    @abstractmethod
    def get_md5_dict(self):
        pass

    def get_ancestors(self, item: DisplayNode, stop_before_func: Callable[[DisplayNode], bool] = None) -> Deque[DisplayNode]:
        ancestors: Deque[DisplayNode] = deque()

        # Walk up the source tree, adding ancestors as we go, until we reach either a node which has already
        # been added to this tree, or the root of the source tree
        ancestor = item
        while ancestor:
            if stop_before_func is not None and stop_before_func(ancestor):
                return ancestors
            ancestor = self.get_parent_for_item(ancestor)
            if ancestor:
                if ancestor.uid == self.uid:
                    # do not include source tree's root node:
                    return ancestors
                ancestors.appendleft(ancestor)

        return ancestors

    # Stats
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @abstractmethod
    def get_summary(self):
        pass

    def refresh_stats(self, tree_id):
        logger.debug(f'Refreshing stats for display tree: "{tree_id}"')
        stats_sw = Stopwatch()
        queue: Deque[DisplayNode] = deque()
        stack: Deque[DisplayNode] = deque()
        queue.append(self.root_node)
        stack.append(self.root_node)

        # go down tree, zeroing out existing stats and adding children to stack
        while len(queue) > 0:
            item: DisplayNode = queue.popleft()
            item.zero_out_stats()

            children = self.get_children(item)
            if children:
                for child in children:
                    if child.is_dir():
                        assert isinstance(child, DisplayNode)
                        queue.append(child)
                        stack.append(child)

        # now go back up the tree by popping the stack and building stats as we go:
        while len(stack) > 0:
            item = stack.pop()
            assert item.is_dir()

            children = self.get_children(item)
            if children:
                if tree_id == actions.ID_RIGHT_TREE: # TODO
                    logger.info(f'Adding metrics for {len(children)} children for node UID {item.uid}: "{item.name}"')
                for child in children:
                    item.add_meta_metrics(child)

        self._stats_loaded = True
        dispatcher.send(signal=actions.REFRESH_SUBTREE_STATS_DONE, sender=tree_id)
        logger.debug(f'[{tree_id}] {stats_sw} Refreshed stats for tree')
