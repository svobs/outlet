import collections
from typing import Deque, List, Optional
import treelib
from index.uid.uid import UID
from model.change_action import ChangeAction


# CLASS DepTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DepTree(treelib.Tree):
    """Dependency tree, currently with emphasis on ChangeActions"""

    def __init__(self):
        treelib.Tree.__init__(self)
        # TODO: rework this
        self.model_command_dict: Dict[UID, Command] = {}
        """Convenient up-to-date mapping for DisplayNode UID -> Command (also allows for context menus to cancel commands!)"""


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
