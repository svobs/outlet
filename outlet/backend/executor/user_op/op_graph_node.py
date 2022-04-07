import collections
import logging
import pathlib
from abc import ABC, abstractmethod
from typing import Any, Deque, Dict, List, Optional, Set

from constants import OP_TREE_INDENT_STR, SUPER_ROOT_UID
from model.node.node import AbstractNode, TNode
from model.uid import UID
from model.user_op import UserOp, UserOpStatus, UserOpCode

logger = logging.getLogger(__name__)


def propagate_status_to_child(parent, child):
    # If error status, propagate it downstream:
    if parent.op and parent.op.is_stopped_on_error():
        child.op.set_status(UserOpStatus.BLOCKED_BY_ERROR)


class OpGraphNode(AbstractNode, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS OpGraphNode

    This is a node which represents an operation (or half of an operation, if the operations includes both src and dst nodes)
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: Optional[UserOp], target_ancestor_uid_list: List[UID]):
        AbstractNode.__init__(self)

        self.node_uid: UID = uid
        """This is the UID of the OpGraphNode, NOT either of the TNode objects inside its UserOp"""

        self.op: UserOp = op
        """The UserOp (i.e. "operation")"""

        self.tgt_ancestor_uid_list: List[UID] = target_ancestor_uid_list

    @property
    def identifier(self):
        return self.node_uid

    @abstractmethod
    def get_tgt_node(self) -> Optional[TNode]:
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

    @staticmethod
    def get_parent_list() -> Optional[List]:
        """Returns a list of this OGN's parent OGN(s)"""
        return []

    @staticmethod
    def get_child_list() -> Optional[List]:
        """Returns a list of this OGN's child OGN(s)"""
        return []

    def link_parent(self, parent):
        raise RuntimeError('Cannot link parent to this class: class cannot have parents!')

    def unlink_parent(self, parent):
        raise RuntimeError('Cannot unlink parent from class: class cannot have parents!')

    def link_child(self, child):
        raise RuntimeError('Cannot link child to this class: class cannot have children!')

    def unlink_child(self, child):
        raise RuntimeError('Cannot unlink child from class: class cannot have children!')

    def is_child_of_root(self) -> bool:
        return not self.is_root() and len(self.get_parent_list()) == 1 and self.get_first_parent().is_root()

    def is_child_of(self, other_ogn) -> bool:
        return not self.is_root() and other_ogn.node_uid in [p.node_uid for p in self.get_parent_list()]

    def is_in_same_batch(self, other_ogn) -> bool:
        return other_ogn.op.batch_uid == self.op.batch_uid

    @abstractmethod
    def clear_relationships(self):
        pass

    @classmethod
    def is_root(cls) -> bool:
        return False

    @classmethod
    def is_src(cls) -> bool:
        # Default is src
        return True

    @classmethod
    def is_dst(cls) -> bool:
        return False

    @abstractmethod
    def is_reentrant(self) -> bool:
        return False

    @classmethod
    def is_rm_node(cls) -> bool:
        return False

    def is_remove_type(self) -> bool:
        return False

    def is_create_type(self) -> bool:
        """Is the target of this OGN going to be created/updated? (i.e. not removed and not just read)"""
        return False

    def get_level(self) -> int:
        max_parent = 0
        for parent in self.get_parent_list():
            parent_level = parent.get_level()
            if parent_level > max_parent:
                max_parent = parent_level

        return max_parent + 1

    def print_me(self, full=True) -> str:
        string = f'(?) og_node_uid={self.node_uid}'
        if full:
            return f'{string}: {self.op}'
        else:
            return string

    def __repr__(self):
        return self.print_me()

    def print_recursively(self, current_level: int = 0, coverage_dict: Dict[UID, Any] = None) -> List[str]:
        if not coverage_dict:
            coverage_dict = {}

        if coverage_dict.get(self.node_uid, None):
            # already printed this node
            return [f'{OP_TREE_INDENT_STR * current_level}[L{current_level}] {self.print_me(full=False)} [see above]']

        coverage_dict[self.node_uid] = self

        blocks = [f'{OP_TREE_INDENT_STR * current_level}[L{current_level}] {self.print_me()}']

        for child in self.get_child_list():
            blocks += child.print_recursively(current_level + 1, coverage_dict)

        return blocks

    def get_subgraph_bfs_list(self, coverage_dict: Dict[UID, Any] = None) -> List:
        """
        Returns: a list of all the nodes in this sutree (including this one) in breadth-first order.
        Since this is a graph (with some nodes having multiple parents) and not a tree, we have the additional condition that
        nodes which have multiple parents are not included until after all of its parents.
        """
        ogn_bfs_list: List[OpGraphNode] = []

        if not coverage_dict:
            coverage_dict = {self.node_uid: self}

        queue: Deque[OpGraphNode] = collections.deque()
        queue.append(self)

        while len(queue) > 0:
            ogn: OpGraphNode = queue.popleft()
            ogn_bfs_list.append(ogn)

            logger.debug(f'get_subgraph_bfs_list(): Added OGN (will now examine its children): {ogn}')

            for child_ogn in ogn.get_child_list():
                logger.debug(f'get_subgraph_bfs_list(): Examining child OGN: {child_ogn}')
                logger.debug(f'get_subgraph_bfs_list(): Child OGN has parent OGNs: {[p.node_uid for p in child_ogn.get_parent_list()]}')
                all_parents_seen = True
                for parent_of_child in child_ogn.get_parent_list():
                    if not coverage_dict.get(parent_of_child.node_uid, None):
                        assert parent_of_child.is_root() or parent_of_child.node_uid != ogn.node_uid,\
                            f'Expected a new parent for {child_ogn} but found existing {ogn}'
                        assert len(child_ogn.get_parent_list()) > 1, \
                            f'Expected child_ogn OGN ({child_ogn}) to have multiple parents but found {child_ogn.get_parent_list()}'
                        logger.debug(f'get_subgraph_bfs_list(): Child OGN {child_ogn.node_uid} has a parent OGN we have not yet encountered '
                                     f'({parent_of_child.node_uid}); skipping for now')
                        all_parents_seen = False

                if all_parents_seen:
                    # avoid duplicates:
                    if not coverage_dict.get(child_ogn.node_uid, None):
                        coverage_dict[child_ogn.node_uid] = child_ogn

                        logger.debug(f'get_subgraph_bfs_list(): Examining children of OGN: {ogn}')
                        queue.append(child_ogn)

        return ogn_bfs_list

    def is_tgt_an_ancestor_of_og_node_tgt(self, other_og_node):
        my_path_list = self.get_tgt_node().get_path_list()
        for other_tgt_path in other_og_node.get_tgt_node().get_path_list():
            for tgt_path in my_path_list:
                if pathlib.PurePosixPath(other_tgt_path).is_relative_to(tgt_path):
                    return True

        return False

    def get_all_downstream_leaves(self) -> List:
        """Includes self, if self has no children."""
        leaf_list: List[OpGraphNode] = []

        queue: Deque[OpGraphNode] = collections.deque()
        queue.append(self)
        coverage_set: Set[UID] = set()

        while len(queue) > 0:
            ogn: OpGraphNode = queue.popleft()

            if not ogn.get_child_list():
                leaf_list.append(ogn)
            else:
                for child_ogn in ogn.get_child_list():
                    if child_ogn.node_uid not in coverage_set:
                        coverage_set.add(child_ogn.node_uid)
                        if child_ogn.get_child_list():
                            leaf_list.append(child_ogn)
                        else:
                            queue.append(child_ogn)

        logger.debug(f'Returning {len(leaf_list)} downstream leaf OGNs for OGN {self}')
        return leaf_list


class HasSingleParent(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    TRAIT HasSingleParent: An OpGraphNode which has exactly 1 parent (unless it's a RootNode).
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        self._parent: Optional[OpGraphNode] = None

    def get_parent_list(self) -> Optional[List]:
        if self._parent:
            return [self._parent]
        return []

    def link_parent(self, parent: OpGraphNode):
        if self._parent:
            if self._parent.node_uid != parent.node_uid:
                raise RuntimeError(f'Cannot link parent (to UID {parent.node_uid}): HasSingleParent already has a different parent linked '
                                   f'(to UID {self._parent.node_uid})!')
        else:
            self._parent = parent
            parent.link_child(self)

    def unlink_parent(self, parent: OpGraphNode):
        if self._parent:
            if self._parent.node_uid == parent.node_uid:
                self._parent = None
                parent.unlink_child(self)
            else:
                raise RuntimeError(f'Cannot unlink parent: given parent ({parent}) does not match actual parent ({self._parent})')

    def clear_relationships(self):
        self._parent = None


class HasMultiParent(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    TRAIT HasMultiParent
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
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
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    TRAIT HasSingleChild
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        self._child: Optional[OpGraphNode] = None

    def get_child_list(self) -> Optional[List]:
        if self._child:
            return [self._child]
        return []

    def link_child(self, child: OpGraphNode):
        if self._child:
            if self._child.node_uid != child.node_uid:
                raise RuntimeError(f'Cannot link child: only a single child allowed! (existing child={self._child}; requested child={child}; '
                                   f'self={self})')
        else:
            self._child = child
            child.link_parent(self)

            propagate_status_to_child(self, child)

    def unlink_child(self, child: OpGraphNode):
        if self._child == child:
            if self._child.node_uid == child.node_uid:
                self._child = None
                child.unlink_parent(self)
            else:
                raise RuntimeError(f'unlink_child(): given child ({child}) does not match actual child ({self._child}) (self={self})')

    def clear_relationships(self):
        self._child = None


class HasMultiChild(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    TRAIT HasMultiChild
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self):
        self._child_dict: Dict[UID, OpGraphNode] = {}

    def get_child_list(self) -> Optional[List]:
        return list(self._child_dict.values())

    def link_child(self, child: OpGraphNode):
        if not self._child_dict.get(child.node_uid, None):
            self._child_dict[child.node_uid] = child
            child.link_parent(self)

            propagate_status_to_child(self, child)

    def unlink_child(self, child: OpGraphNode):
        if self._child_dict.get(child.node_uid, None):
            self._child_dict.pop(child.node_uid)
            child.unlink_parent(self)

    def clear_relationships(self):
        self._child_dict.clear()


class RootNode(HasMultiChild, OpGraphNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RootNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self):
        OpGraphNode.__init__(self, SUPER_ROOT_UID, None, [])
        HasMultiChild.__init__(self)

    @classmethod
    def is_root(cls):
        return True

    def is_reentrant(self) -> bool:
        return True

    def get_tgt_node(self) -> Optional[TNode]:
        return None

    def clear_relationships(self):
        HasMultiChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        return f'ROOT_no_op'


class SrcOpNode(HasMultiParent, HasMultiChild, OpGraphNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SrcOpNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp, target_ancestor_uid_list: List[UID]):
        OpGraphNode.__init__(self, uid, op=op, target_ancestor_uid_list=target_ancestor_uid_list)
        HasMultiParent.__init__(self)
        HasMultiChild.__init__(self)

    def is_reentrant(self) -> bool:
        # Only CP src nodes are reentrant
        return self.op.op_type == UserOpCode.CP

    def get_tgt_node(self) -> Optional[TNode]:
        return self.op.src_node

    def is_create_type(self) -> bool:
        return self.op.op_type == UserOpCode.MKDIR

    def is_remove_type(self) -> bool:
        # remember, the src node of a MV gets removed
        return self.op.op_type == UserOpCode.MV or self.op.op_type == UserOpCode.MV_ONTO or self.op.op_type == UserOpCode.FINISH_DIR_MV

    def clear_relationships(self):
        HasMultiParent.clear_relationships(self)
        HasMultiChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        string = f'SRC-{self.op.op_type.name} ogn_uid={self.node_uid}'
        if full:
            return f'{string} {self.op}'
        else:
            return string


class DstOpNode(HasMultiParent, HasMultiChild, OpGraphNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DstOpNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp, target_ancestor_uid_list: List[UID]):
        assert op.has_dst()
        OpGraphNode.__init__(self, uid, op=op, target_ancestor_uid_list=target_ancestor_uid_list)
        HasMultiParent.__init__(self)
        HasMultiChild.__init__(self)

    def is_reentrant(self) -> bool:
        return False

    def get_tgt_node(self) -> Optional[TNode]:
        return self.op.dst_node

    @classmethod
    def is_src(cls) -> bool:
        return False

    @classmethod
    def is_dst(cls):
        return True

    def is_create_type(self) -> bool:
        return True

    def clear_relationships(self):
        HasMultiParent.clear_relationships(self)
        HasMultiChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        string = f'DST-{self.op.op_type.name} ogn_uid={self.node_uid}'
        if full:
            return f'{string} {self.op}'
        else:
            return string


class RmOpNode(HasMultiParent, HasSingleChild, OpGraphNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RmOpNode

    RM nodes have an inverted structure: the child nodes become the shared parents for their parent dir.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp, target_ancestor_uid_list: List[UID]):
        assert op.op_type == UserOpCode.RM
        OpGraphNode.__init__(self, uid, op=op, target_ancestor_uid_list=target_ancestor_uid_list)
        HasMultiParent.__init__(self)
        HasSingleChild.__init__(self)

    @classmethod
    def is_rm_node(cls) -> bool:
        return True

    def is_remove_type(self) -> bool:
        return True

    def is_reentrant(self) -> bool:
        return False

    def get_tgt_node(self):
        return self.op.src_node

    def clear_relationships(self):
        HasMultiParent.clear_relationships(self)
        HasSingleChild.clear_relationships(self)

    def print_me(self, full=True) -> str:
        string = f'RM ogn_uid={self.node_uid}'
        if full:
            return f'{string}: {self.op}'
        else:
            return string
