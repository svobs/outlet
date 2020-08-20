import collections
import logging
from abc import ABC, abstractmethod
from typing import Any, Deque, Dict, Iterable, List, Optional

from constants import OP_TREE_INDENT_STR, ROOT_UID
from index.uid.uid import UID
from model.change_action import ChangeAction, ChangeType

logger = logging.getLogger(__name__)


# ABSTRACT CLASS OpGraphNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class OpGraphNode(ABC):
    """This is a node which represents an operation (or half of an operation, if the operations includes both src and dst nodes)"""
    def __init__(self, uid: UID, change_action: Optional[ChangeAction]):
        self.node_uid: UID = uid
        self.change_action: ChangeAction = change_action
        """The ChangeAction (i.e. "operation")"""

    @property
    def identifier(self):
        return self.node_uid

    @abstractmethod
    def get_target_node(self):
        pass

    def get_first_parent(self) -> Optional:
        """Returns one of the parents, or None if there aren't any"""
        try:
            parent_list: Optional[List] = self.get_parent_list()
            if parent_list:
                return next(iter(parent_list))
        except StopIteration:
            pass
        return None

    def get_first_child(self) -> Optional:
        """Returns one of the children, or None if there aren't any"""
        try:
            child_list: Optional[List] = self.get_child_list()
            if child_list:
                return next(iter(child_list))
        except StopIteration:
            pass
        return None

    def get_parent_list(self) -> Optional[List]:
        return None

    def get_child_list(self) -> Optional[List]:
        return None

    def link_parent(self, parent):
        raise RuntimeError('Cannot link parent to this class: class cannot have parents!')

    def unlink_parent(self, parent):
        raise RuntimeError('Cannot unlink parent from class: class cannot have parents!')

    def link_child(self, child):
        raise RuntimeError('Cannot link child to this class: class cannot have children!')

    def unlink_child(self, child):
        raise RuntimeError('Cannot unlink child from class: class cannot have children!')

    @abstractmethod
    def clear_relationships(self):
        pass

    @classmethod
    def is_root(cls) -> bool:
        return False

    @classmethod
    def is_src(cls) -> bool:
        return True

    @classmethod
    def is_dst(cls) -> bool:
        return False

    @abstractmethod
    def is_mutually_exclusive(self) -> bool:
        return False

    def is_remove_type(self) -> bool:
        return False

    def is_create_type(self) -> bool:
        return False

    def get_level(self) -> int:
        level: int = 0
        node = self
        while node:
            level += 1
            node = node.get_first_parent()

        return level

    def print_me(self, full=True) -> str:
        string = f'(?) op node UID={self.node_uid}'
        if full:
            return f'{string}: {self.change_action}'
        else:
            return string

    def __repr__(self):
        return self.print_me()

    def print_recursively(self, coverage_dict: Dict[UID, Any] = None) -> List[str]:
        if not coverage_dict:
            coverage_dict = {}

        level = self.get_level()
        if coverage_dict.get(self.node_uid, None):
            # already printed this node
            return [f'{OP_TREE_INDENT_STR * (level-1)}{self.print_me(full=False)} [see above]']

        coverage_dict[self.node_uid] = self

        blocks = [f'{OP_TREE_INDENT_STR * (level-1)}{self.print_me()}']

        for child in self.get_child_list():
            blocks += child.print_recursively(coverage_dict)

        return blocks

    def get_all_nodes_in_subtree(self, coverage_dict: Dict[UID, Any] = None) -> List:
        """
        Returns: a list of all the nodes in this sutree (including this one) in breadth-first order
        """
        if not coverage_dict:
            coverage_dict = {}

        node_list = []

        queue: Deque[OpGraphNode] = collections.deque()
        queue.append(self)

        while len(queue) > 0:
            node: OpGraphNode = queue.popleft()

            node_list.append(node)

            for child in node.get_child_list():
                # avoid duplicates:
                if not coverage_dict.get(child.node_uid, None):
                    coverage_dict[child.node_uid] = child
                    queue.append(child)

        return node_list


# TRAIT HasSingleParent
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class HasSingleParent(ABC):
    def __init__(self):
        self._parent: Optional[OpGraphNode] = None

    def get_parent_list(self) -> Optional[List]:
        if self._parent:
            return [self._parent]
        return []

    def link_parent(self, parent: OpGraphNode):
        if self._parent:
            if self._parent.node_uid != parent.node_uid:
                raise RuntimeError('Cannot link parent: HasSingleParent already has a different parent linked!')
        else:
            self._parent = parent
            parent.link_child(self)

    def unlink_parent(self, parent: OpGraphNode):
        if self._parent:
            if self._parent.node_uid == parent.node_uid:
                self._parent = None
                parent.unlink_child(self)
            else:
                raise RuntimeError(f'unlink_parent(): given parent ({parent}) does not match actual parent ({self._parent})')

    def clear_relationships(self):
        self._parent = None


# TRAIT HasMultiParent
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class HasMultiParent(ABC):
    def __init__(self):
        self._parent_dict: Dict[UID, OpGraphNode] = {}

    def get_parent_list(self) -> Optional[List]:
        return list(self._parent_dict.values())

    def link_parent(self, parent: OpGraphNode):
        if not self._parent_dict.get(parent.node_uid, None):
            self._parent_dict[parent.node_uid] = parent
            parent.link_child(self)

    def unlink_parent(self, parent: OpGraphNode):
        if self._parent_dict.get(parent.node_uid, None):
            self._parent_dict.pop(parent.node_uid)
            parent.unlink_child(self)

    def clear_relationships(self):
        self._parent_dict.clear()


# TRAIT HasSingleChild
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class HasSingleChild(ABC):
    def __init__(self):
        self._child: Optional[OpGraphNode] = None

    def get_child_list(self) -> Optional[List]:
        if self._child:
            return [self._child]
        return []

    def link_child(self, child: OpGraphNode):
        if self._child:
            if self._child.node_uid != child.node_uid:
                raise RuntimeError('Only a single child allowed!')
        else:
            self._child = child
            child.link_parent(self)

    def unlink_child(self, child: OpGraphNode):
        if self._child == child:
            if self._child.node_uid == child.node_uid:
                self._child = None
                child.unlink_parent(self)
            else:
                raise RuntimeError(f'unlink_child(): given child ({child}) does not match actual child ({self._child})')

    def clear_relationships(self):
        self._child = None


# TRAIT HasMultiChild
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class HasMultiChild(ABC):
    def __init__(self):
        self._child_dict: Dict[UID, OpGraphNode] = {}

    def get_child_list(self) -> Optional[List]:
        return list(self._child_dict.values())

    def link_child(self, child: OpGraphNode):
        if not self._child_dict.get(child.node_uid, None):
            self._child_dict[child.node_uid] = child
            child.link_parent(self)

    def unlink_child(self, child: OpGraphNode):
        if self._child_dict.get(child.node_uid, None):
            self._child_dict.pop(child.node_uid)
            child.unlink_parent(self)

    def clear_relationships(self):
        self._child_dict.clear()


# CLASS RootNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class RootNode(HasMultiChild, OpGraphNode):

    def __init__(self):
        OpGraphNode.__init__(self, ROOT_UID, None)
        HasMultiChild.__init__(self)

    @classmethod
    def is_root(cls):
        return True

    def is_mutually_exclusive(self) -> bool:
        return True

    def get_target_node(self):
        return None

    def clear_relationships(self):
        HasMultiChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        return f'ROOT_no_op'


# CLASS SrcOpNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SrcOpNode(HasSingleParent, HasMultiChild, OpGraphNode):
    def __init__(self, uid: UID, change_action: ChangeAction):
        OpGraphNode.__init__(self, uid, change_action=change_action)
        HasSingleParent.__init__(self)
        HasMultiChild.__init__(self)

    def is_mutually_exclusive(self) -> bool:
        # Only CP src nodes are non-mutually-exclusive
        return self.change_action.change_type != ChangeType.CP

    def get_target_node(self):
        return self.change_action.src_node

    def is_create_type(self) -> bool:
        return self.change_action.change_type == ChangeType.MKDIR

    def clear_relationships(self):
        HasSingleParent.clear_relationships(self)
        HasMultiChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        string = f'SRC_op UID={self.node_uid}'
        if full:
            return f'{string}: {self.change_action}'
        else:
            return string


# CLASS DstOpNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DstOpNode(HasSingleParent, HasMultiChild, OpGraphNode):
    def __init__(self, uid: UID, change_action: ChangeAction):
        assert change_action.has_dst()
        OpGraphNode.__init__(self, uid, change_action=change_action)
        HasSingleParent.__init__(self)
        HasMultiChild.__init__(self)

    def is_mutually_exclusive(self) -> bool:
        return True

    def get_target_node(self):
        return self.change_action.dst_node

    @classmethod
    def is_src(cls) -> bool:
        return False

    @classmethod
    def is_dst(cls):
        return True

    def is_create_type(self) -> bool:
        return True

    def clear_relationships(self):
        HasSingleParent.clear_relationships(self)
        HasMultiChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        string = f'DST_op UID={self.node_uid}'
        if full:
            return f'{string}: {self.change_action}'
        else:
            return string


# CLASS RmOpNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class RmOpNode(HasMultiParent, HasSingleChild, OpGraphNode):
    """RM nodes have an inverted structure: the child nodes become the shared parents for their parent dir."""
    def __init__(self, uid: UID, change_action: ChangeAction):
        assert change_action.change_type == ChangeType.RM
        OpGraphNode.__init__(self, uid, change_action=change_action)
        HasMultiParent.__init__(self)
        HasSingleChild.__init__(self)

    def is_remove_type(self) -> bool:
        return True

    def is_mutually_exclusive(self) -> bool:
        return True

    def get_target_node(self):
        return self.change_action.src_node

    def clear_relationships(self):
        HasMultiParent.clear_relationships(self)
        HasSingleChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        string = f'RM_op UID={self.node_uid}'
        if full:
            return f'{string}: {self.change_action}'
        else:
            return string
