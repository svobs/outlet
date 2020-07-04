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
