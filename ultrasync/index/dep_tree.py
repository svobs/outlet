import collections
from typing import Deque, List
from command.command_interface import Command
import treelib


# CLASS DepTree
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class DepTree(treelib.Tree):
    """Dependency tree, currently with emphasis on ChangeActions"""

    def get_breadth_first_list(self):
        """Returns the command tree as a list, in breadth-first order"""
        blist: List[Command] = []

        queue: Deque[Command] = collections.deque()
        # skip root:
        for child in self.tree.children(self.tree.root):
            queue.append(child)

        while len(queue) > 0:
            item: Command = queue.popleft()
            blist.append(item)
            for child in self.tree.children(item.identifier):
                queue.append(child)

        return blist
