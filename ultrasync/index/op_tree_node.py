import collections
import logging
from abc import ABC, abstractmethod
from typing import Deque, List, Optional

from constants import ROOT_UID
from index.uid.uid import UID
from model.change_action import ChangeAction, ChangeType

logger = logging.getLogger(__name__)


# ABSTRACT CLASS OpTreeNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class OpTreeNode(ABC):
    """This is a node which represents an operation (or half of an operation, if the operations includes both src and dst nodes)"""
    def __init__(self, uid: UID, change_action: Optional[ChangeAction]):
        self.node_uid: UID = uid
        self.change_action: ChangeAction = change_action
        """The ChangeAction (i.e. "operation")"""
        self.children: List[OpTreeNode] = []
        self.parent: Optional[OpTreeNode] = None

    @property
    def identifier(self):
        return self.node_uid

    @abstractmethod
    def get_target_node(self):
        pass

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def remove_child(self, child):
        self.children.remove(child)
        if child.parent == self:
            child.parent = None

    @classmethod
    def is_root(cls) -> bool:
        return False

    @classmethod
    def is_dst(cls) -> bool:
        return False

    def is_create_type(self) -> bool:
        return False

    def get_level(self) -> int:
        level: int = 0
        node = self
        while node:
            level += 1
            node = node.parent

        return level

    def get_all_nodes_in_subtree(self):
        """
        Returns: a list of all the nodes in this sutree (including this one) in breadth-first order
        """
        node_list = []

        queue: Deque[OpTreeNode] = collections.deque()
        queue.append(self)

        while len(queue) > 0:
            node: OpTreeNode = queue.popleft()
            node_list.append(node)

            for child in node.children:
                queue.append(child)

        return node_list


# CLASS RootNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class RootNode(OpTreeNode):
    def __init__(self):
        super().__init__(ROOT_UID, None)

    @classmethod
    def is_root(cls):
        return True

    def get_target_node(self):
        return None


# CLASS SrcActionNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SrcActionNode(OpTreeNode):
    def __init__(self, uid: UID, change_action: ChangeAction):
        super().__init__(uid, change_action=change_action)

    def get_target_node(self):
        return self.change_action.src_node

    def is_create_type(self) -> bool:
        return self.change_action.change_type == ChangeType.MKDIR


# CLASS DstActionNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DstActionNode(OpTreeNode):
    def __init__(self, uid: UID, change_action: ChangeAction):
        assert change_action.has_dst()
        super().__init__(uid, change_action=change_action)

    def get_target_node(self):
        return self.change_action.dst_node

    @classmethod
    def is_dst(cls):
        return True

    def is_create_type(self) -> bool:
        return True