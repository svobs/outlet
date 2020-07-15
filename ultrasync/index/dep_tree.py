import collections
import pathlib
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import DefaultDict, Deque, Dict, Iterable, List, Optional

from constants import ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.uid.uid import UID
from model.change_action import ChangeAction, ChangeType
from model.node.display_node import DisplayNode, HasParentList
from model.node.gdrive_node import GDriveNode

logger = logging.getLogger(__name__)

# class DepType(IntEnum):
#     NONE = 1
#     UPSTREAM = 2
#     DOWNSTREAM = 3


# ABSTRACT CLASS TreeNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class TreeNode(ABC):
    def __init__(self, uid: UID, change_action: Optional[ChangeAction]):
        self.node_uid: UID = uid
        self.change_action: ChangeAction = change_action
        self.children: List[TreeNode] = []
        self.parent: Optional[TreeNode] = None

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
    def is_root(cls):
        return False

    @classmethod
    def is_dst(cls):
        return False


# CLASS RootNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class RootNode(TreeNode):
    def __init__(self):
        super().__init__(ROOT_UID, None)

    @classmethod
    def is_root(cls):
        return True

    def get_target_node(self):
        return None


# CLASS SrcActionNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class SrcActionNode(TreeNode):
    def __init__(self, uid: UID, change_action: ChangeAction):
        super().__init__(uid, change_action=change_action)

    def get_target_node(self):
        return self.change_action.src_node



# CLASS DstActionNode
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DstActionNode(TreeNode):
    def __init__(self, uid: UID, change_action: ChangeAction):
        assert change_action.has_dst()
        super().__init__(uid, change_action=change_action)

    def get_target_node(self):
        return self.change_action.dst_node

    @classmethod
    def is_dst(cls):
        return True


# CLASS DepTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DepTree:
    """Dependency tree, currently with emphasis on ChangeActions"""

    def __init__(self, cache_manager, uid_generator):
        self.cacheman = cache_manager
        self.uid_generator = uid_generator
        self.node_dict: DefaultDict[UID, Deque[TreeNode]] = collections.defaultdict(lambda: collections.deque())
        self.root: TreeNode = RootNode()

    def _derive_parent_uid(self, node: DisplayNode):
        """Derives the UID for the parent of the given node"""
        if isinstance(node, HasParentList):
            assert isinstance(node, GDriveNode) and node.node_identifier.tree_type == TREE_TYPE_GDRIVE, f'Node: {node}'
            parent_uids = node.get_parent_uids()
            assert len(parent_uids) == 1, f'Expected exactly one parent_uid for node: {node}'
            return parent_uids[0]
        else:
            assert node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK, f'Node: {node}'
            parent_path = str(pathlib.Path(node.full_path).parent)
            return self.cacheman.get_uid_for_path(parent_path)

    # TODO: junk code ?
    # def _determine_relationship(self, node_to_insert: TreeNode, tree_node: TreeNode):
    #     node: DisplayNode = node_to_insert.get_target_node()
    #
    #     tree_ancestor: DisplayNode = tree_node.get_target_node()
    #     # check initial node:
    #     if tree_ancestor.uid == node.uid:
    #         raise RuntimeError(f'Duplicate node: {node_to_insert}')
    #
    #     node_parent_uid = self._derive_parent_uid(node)
    #     if tree_ancestor.uid == node_parent_uid:
    #         return DepType.DOWNSTREAM
    #
    #     # See if node to be inserted is upstream of the tree_node. Do this by iterating up the actual tree
    #     while tree_ancestor:
    #         tree_ancestor = self.cacheman.get_parent_for_item(tree_ancestor)
    #         if tree_ancestor and tree_ancestor.uid == node.uid:
    #             return DepType.UPSTREAM
    #
    #     return DepType.NONE
    #
    # def _insert_node(self, node_to_insert: TreeNode, root_node: RootNode):
    #     queue: Deque = collections.deque()
    #     queue.append(root_node)
    #
    #     while len(queue) > 0:
    #         inserted = False
    #         parent_node: TreeNode = queue.popleft()
    #         # We can always assume that node_to_insert is downstream of parent_node
    #         for child in parent_node.children:
    #             dep = self._determine_relationship(node_to_insert, child)
    #             if dep == DepType.UPSTREAM:
    #                 # Insert between parent_node and child: swap child and node_to_insert
    #                 node_to_insert.add_child(child)
    #                 parent_node.remove_child(child)
    #                 if not inserted:
    #                     # Only do this once per parent/node_to_insert combo
    #                     parent_node.add_child(node_to_insert)
    #                 inserted = True
    #                 # Fall through. Need to do this check for all children.
    #             elif dep == DepType.DOWNSTREAM:
    #                 # Drill down into child. All other children can be ignored
    #                 queue.append(child)
    #                 continue
    #             elif dep == DepType.NONE:
    #                 pass
    #         if not inserted:
    #             parent_node.add_child(node_to_insert)
    #         return
    #
    # def _get_all_nodes(self, subtree_root: TreeNode):
    #     node_list = []
    #
    #     queue: Deque[TreeNode] = collections.deque()
    #     queue.append(subtree_root)
    #
    #     while len(queue) > 0:
    #         node: TreeNode = queue.popleft()
    #         node_list.append(node)
    #
    #         for child in node.children:
    #             queue.append(child)
    #
    #     return node_list

    def make_tree_to_insert(self, change_batch: Iterable[ChangeAction]) -> RootNode:
        # Verify batch sort:
        last_uid = 0
        for change in change_batch:
            # Changes MUST be sorted in ascending time of creation!
            if change.action_uid < last_uid:
                raise RuntimeError(f'Batch items are not in order! ({change.action_uid} < {last_uid}')
            last_uid = change.action_uid

        # 1. Put all in dict as wrapped TreeNodes
        node_dict: Dict[UID, TreeNode] = {}
        for change in change_batch:
            src_node = SrcActionNode(self.uid_generator.next_uid(), change)
            existing = node_dict.get(src_node.get_target_node().uid, None)
            if existing:
                raise RuntimeError(f'Duplicate node: {src_node.get_target_node()}')
            node_dict[src_node.get_target_node().uid] = src_node

            if change.has_dst():
                dst_node = DstActionNode(self.uid_generator.next_uid(), change)
                existing = node_dict.get(dst_node.get_target_node().uid, None)
                if existing:
                    raise RuntimeError(f'Duplicate node: {dst_node.get_target_node()}')
                node_dict[dst_node.get_target_node().uid] = dst_node

        # Assemble nodes one by one with parent-child relationships.
        root_node = RootNode()
        for potential_child_node in node_dict.values():
            parent_uid = self._derive_parent_uid(potential_child_node.get_target_node())
            parent = node_dict.get(parent_uid, None)
            if parent:
                parent.add_child(potential_child_node)
            else:
                # those with no parent will be children of root:
                root_node.add_child(potential_child_node)

        # TODO: print out contents of tree
        return root_node

    def can_add_batch(self, root_of_changes: RootNode) -> bool:
        """
        Takes a tree representing a batch as an arg. The root itself is ignored, but each of its children represent the root of a
        subtree of changes, in which each node of the subtree maps to a node in a directory tree. No intermediate nodes are allowed to be
        omitted from a subtree (e.g. if A is the parent of B which is the parent of C, you cannot copy A and C but exclude B).

        Rules:
        1. Parent for MKDIR_SRC and all DST nodes must be present in tree and not already scheduled for RM
        2. Except for MKDIR, SRC nodes must all be present in tree (OK if exists==False due to pending operation)
        and not already scheduled for RM
        """

        # Invert RM nodes when inserting into tree
        batch_uid = root_of_changes.children[0].change_action.batch_uid

        for change_subtree_root in root_of_changes.children:
            subtree_root_node: DisplayNode = change_subtree_root.get_target_node()
            op_type: str = change_subtree_root.change_action.change_type.name
            if change_subtree_root.change_action.change_type == ChangeType.MKDIR or change_subtree_root.is_dst():
                # Enforce Rule 1: ensure parent is valid:
                parent_uid = self._derive_parent_uid(subtree_root_node)
                if not self.cacheman.get_item_for_uid(parent_uid):
                    logger.error(f'Could not find parent in cache with UID {parent_uid} for "{op_type}" operation node: {subtree_root_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find parent in cache with UID {parent_uid} '
                                       f'for "{op_type}"')
            else:
                # Enforce Rule 2:
                if not self.cacheman.get_item_for_uid(subtree_root_node.uid):
                    logger.error(f'Could not find node in cache for "{op_type}" operation node: {subtree_root_node}')
                    raise RuntimeError(f'Cannot add batch (UID={batch_uid}): Could not find node in cache with UID {subtree_root_node.uid} '
                                       f'for "{op_type}"')

            pending_change_list = self.node_dict.get(subtree_root_node.uid, [])
            if pending_change_list:
                for change_node in pending_change_list:
                    # TODO: for now, just deny anything which tries to work off an RM. Refine in future
                    if change_node.change_action.change_type == ChangeType.RM:
                        raise RuntimeError(f'Cannot add batch (UID={batch_uid}): it depends on a node (UID={subtree_root_node.uid}) '
                                           f'which is being removed')

        return True

    def add_batch(self, root_of_changes: RootNode):
        # 1. Discard root
        # 2. Examine each child of root. Each shall be treated as its own subtree.
        # 3. For each subtree, look up all its nodes in the master dict

        # Disregard the kind of change when building the tree; they are all equal
        # Once entire tree is constructed, invert the RM subtree (if any) so that ancestor RMs become descendants
        #

    def get_breadth_first_list(self):
        """Returns the change tree as a list, in breadth-first order"""
        blist: List[ChangeAction] = []

        queue: Deque[ChangeAction] = collections.deque()
        # skip root:
        for child in self.children(self.root):
            queue.append(child)

        while len(queue) > 0:
            item: ChangeAction = queue.popleft()
            blist.append(item)
            for child in self.children(item.action_uid):
                queue.append(child)

        return blist

    def get_item_for_uid(self, uid: UID) -> ChangeAction:
        return self.get_node(uid)

    def get_parent(self, uid: UID) -> Optional[ChangeAction]:
        parent = self.tree.parent(nid=uid)
        if parent and isinstance(parent, ChangeAction):
            return parent
        return None

    def add_change(self, change: ChangeAction):
        # (1) Add model to lookup table (both src and dst if applicable)
        # self.model_command_dict[change_action.src_node.uid] = command
        # if change_action.dst_node:
        #     self.model_command_dict[command.change_action.dst_node.uid] = command

        # FIXME: add master dependency tree logic
        pass

    def get_next_change(self) -> Optional[ChangeAction]:
        """Gets and returns the next available ChangeAction from the tree; returns None if nothing either queued or ready.
        Internally this class keeps track of what this has returned previously, and will expect to be notified when each is complete."""

        # TODO
        pass

    def change_completed(self, change: ChangeAction):
        # TODO: ensure that we were expecting this change

        # TODO: remove change from tree
        pass
