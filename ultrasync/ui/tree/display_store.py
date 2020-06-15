import logging
from typing import Any, Callable, Dict, Optional, Union

import gi
from gi.repository.Gtk import TreeIter, TreePath

from index.uid_generator import UID
from model.display_node import DisplayNode
from model.planning_node import FileDecoratorNode

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


#    CLASS DisplayStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class DisplayStore:
    """(Mostly) encapsulates the nodes inside the TreeView object, which will be a subset of the nodes
    which come from the data store """
    def __init__(self, treeview_meta):
        self.treeview_meta = treeview_meta
        self.model = Gtk.TreeStore()
        self.model.set_column_types(self.treeview_meta.col_types)
        # Sort by name column at program launch:
        self.model.set_sort_column_id(self.treeview_meta.col_num_name, Gtk.SortType.ASCENDING)

        # Track the checkbox states here. For increased speed and to accommodate lazy loading strategies,
        # we employ the following heuristic:
        # - When a user checks a row, it goes in the 'checked_rows' set below.
        # - When it is placed in the checked_rows set, it is implied that all its descendants are also checked.
        # - Similarly, when an item is unchecked by the user, all of its descendants are implied to be unchecked.
        # - HOWEVER, un-checking an item will not delete any descendants that may be in the 'checked_rows' list.
        #   Anything in the 'checked_rows' and 'inconsistent_rows' lists is only relevant if its parent is 'inconsistent',
        #   thus, having a parent which is either checked or unchecked overrides any presence in either of these two lists.
        # - At the same time as an item is checked, the checked & inconsistent state of its all ancestors must be recorded.
        # - The 'inconsistent_rows' list is needed for display purposes.
        self.checked_rows: Dict[UID, DisplayNode] = {}
        self.inconsistent_rows: Dict[UID, DisplayNode] = {}
        self.displayed_rows: Dict[UID, DisplayNode] = {}
        self.displayed_decorated_rows: Dict[UID, DisplayNode] = {}
        """Need to track these so that we can remove a node if its src node was removed"""

    def get_node_data(self, tree_path: Union[TreeIter, TreePath]) -> DisplayNode:
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The data node (the contents of the hidden "data" column for the given row
        """
        return self.model[tree_path][self.treeview_meta.col_num_data]

    def get_node_name(self, tree_path) -> str:
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The value of the 'name' column for the given row
        """
        return self.model[tree_path][self.treeview_meta.col_num_name]

    def is_node_checked(self, tree_path) -> bool:
        return self.model[tree_path][self.treeview_meta.col_num_checked]

    def is_inconsistent(self, tree_path) -> bool:
        return self.model[tree_path][self.treeview_meta.col_num_inconsistent]

    def clear_model(self) -> TreeIter:
        self.model.clear()
        self.checked_rows.clear()
        self.inconsistent_rows.clear()
        self.displayed_rows.clear()
        self.displayed_decorated_rows.clear()
        return self.model.get_iter_first()

    def _set_checked_state(self, tree_iter, is_checked, is_inconsistent):
        assert not (is_checked and is_inconsistent)
        node_data: DisplayNode = self.get_node_data(tree_iter)

        if not node_data.has_path():
            # Cannot be checked if no path (LoadingNode, etc.)
            return

        row = self.model[tree_iter]
        row[self.treeview_meta.col_num_checked] = is_checked
        row[self.treeview_meta.col_num_inconsistent] = is_inconsistent

        self._update_checked_state_tracking(node_data, is_checked, is_inconsistent)

    def _update_checked_state_tracking(self, node_data: DisplayNode, is_checked: bool, is_inconsistent: bool):
        row_id = node_data.identifier
        if is_checked:
            self.checked_rows[row_id] = node_data
        else:
            if row_id in self.checked_rows: del self.checked_rows[row_id]

        if is_inconsistent:
            self.inconsistent_rows[row_id] = node_data
        else:
            if row_id in self.inconsistent_rows: del self.inconsistent_rows[row_id]

    def on_cell_checkbox_toggled(self, widget, path):
        """LISTENER/CALLBACK: Called when checkbox in treeview is toggled"""
        data_node: DisplayNode = self.get_node_data(path)
        if not data_node.has_path():
            logger.debug('Disallowing checkbox toggle because node is ephemereal')
            return
        elif self.treeview_meta.is_ignored_func and self.treeview_meta.is_ignored_func(data_node):
            logger.debug('Disallowing checkbox toggle because node is in IGNORED category')
            return
        checked_value = not self.is_node_checked(path)
        logger.debug(f'Toggled {checked_value}: {self.get_node_name(path)}')

        # Need to update all the siblings (children of parent) because their checked state may not be tracked.
        # We can assume that if a parent is not inconsistent (i.e. is either checked or unchecked), the state of its children are implied.
        # But if the parent is inconsistent, we must track the state of ALL of its children.
        tree_iter = self.model.get_iter(path)
        parent_iter = self.model.iter_parent(tree_iter)
        if parent_iter:
            child_iter = self.model.iter_children(parent_iter)
            while child_iter:
                child_data = self.get_node_data(child_iter)
                child_checked = self.model[child_iter][self.treeview_meta.col_num_checked]
                child_inconsistent = self.model[child_iter][self.treeview_meta.col_num_inconsistent]

                self._update_checked_state_tracking(child_data, child_checked, child_inconsistent)

                child_iter = self.model.iter_next(child_iter)

        # Update all of the node's children to match its check state:
        def update_checked_state(t_iter):
            self._set_checked_state(t_iter, checked_value, False)

        self.do_for_self_and_descendants(path, update_checked_state)

        # Now update its ancestors' states:
        tree_path = Gtk.TreePath.new_from_string(path)
        while True:
            # Go up the tree, one level per loop,
            # with each node updating itself based on its immediate children
            tree_path.up()
            if tree_path.get_depth() < 1:
                # Stop at root
                break
            else:
                tree_iter = self.model.get_iter(tree_path)
                has_checked = False
                has_unchecked = False
                has_inconsistent = False
                child_iter = self.model.iter_children(tree_iter)
                while child_iter is not None:
                    # Parent is inconsistent if any of its children do not match it...
                    if self.is_node_checked(child_iter):
                        has_checked = True
                    else:
                        has_unchecked = True
                    # ...or if any of its children are inconsistent
                    has_inconsistent |= self.is_inconsistent(child_iter)
                    child_iter = self.model.iter_next(child_iter)
                is_checked = has_checked and not has_unchecked and not has_inconsistent
                is_inconsistent = has_inconsistent or (has_checked and has_unchecked)
                self._set_checked_state(tree_iter, is_checked, is_inconsistent)

    # --- Tree searching & iteration (utility functions) --- #

    def _found_func(self, tree_iter, target_uid: UID) -> bool:
        node = self.get_node_data(tree_iter)
        # if logger.isEnabledFor(logging.DEBUG) and not node.is_ephemereal():
        #     logger.debug(f'Examining node uid={node.uid} (looking for: {target_uid})')
        return not node.is_ephemereal() and node.uid == target_uid

    def find_in_tree(self, target_uid: UID, tree_iter: Optional[Gtk.TreeIter] = None) -> Optional[Gtk.TreeIter]:
        """Recurses over entire tree and visits every node until is_found_func() returns True, then returns the data at that node"""
        if not tree_iter:
            tree_iter = self.model.get_iter_first()

        while tree_iter is not None:
            if self._found_func(tree_iter, target_uid):
                return tree_iter
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                ret_iter = self.find_in_tree(target_uid, child_iter)
                if ret_iter:
                    return ret_iter
            tree_iter = self.model.iter_next(tree_iter)
        return None

    def find_in_top_level(self, target_uid: UID):
        """Searches the children of the given parent_iter for the given UID, then returns the data at that node"""
        tree_iter = self.model.get_iter_first()
        while tree_iter is not None:
            if self._found_func(tree_iter, target_uid):
                return tree_iter
            tree_iter = self.model.iter_next(tree_iter)
        return None

    def find_in_children(self, target_uid: UID, parent_iter):
        """Searches the children of the given parent_iter for the given UID, then returns the data at that node"""
        if self.model.iter_has_child(parent_iter):
            child_iter = self.model.iter_children(parent_iter)
            while child_iter is not None:
                if self._found_func(child_iter, target_uid):
                    return child_iter
                child_iter = self.model.iter_next(child_iter)
        return None

    def recurse_over_tree(self, tree_iter: Gtk.TreeIter = None, action_func: Callable[[Gtk.TreeIter], None] = None):
        """
        Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants
        """
        if not tree_iter:
            tree_iter = self.model.get_iter_first()

        while tree_iter is not None:
            action_func(tree_iter)
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.recurse_over_tree(child_iter, action_func)
            tree_iter = self.model.iter_next(tree_iter)

    def do_for_descendants(self, tree_path, action_func):
        tree_iter = self.model.get_iter(tree_path)
        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_self_and_descendants(self, tree_path, action_func):
        tree_iter = self.model.get_iter(tree_path)
        action_func(tree_iter)

        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_subtree_and_following_sibling_subtrees(self, tree_path, action_func):
        """
        Includes self and all descendents, and then does the same for all following siblings and their descendants.
        """
        tree_iter = self.model.get_iter(tree_path)
        self.recurse_over_tree(tree_iter, action_func)

    def append_node(self, parent_node_iter, row_values: list):
        data: DisplayNode = row_values[self.treeview_meta.col_num_data]

        if not data.is_ephemereal():
            if isinstance(data, FileDecoratorNode):
                self.displayed_decorated_rows[data.src_node.uid] = data
            self.displayed_rows[data.uid] = data

        return self.model.append(parent_node_iter, row_values)

    def remove_from_lists(self, uid: UID):
        if uid in self.checked_rows: del self.checked_rows[uid]
        if uid in self.inconsistent_rows: del self.inconsistent_rows[uid]
        if uid in self.displayed_rows: del self.displayed_rows[uid]
        if uid in self.displayed_decorated_rows: del self.displayed_decorated_rows[uid]

    def remove_first_child(self, parent_iter):
        first_child_iter = self.model.iter_children(parent_iter)
        if not first_child_iter:
            return False

        child_data = self.get_node_data(first_child_iter)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Removing child: {child_data}')

        if not child_data.is_ephemereal():
            if isinstance(child_data, FileDecoratorNode):
                self.displayed_decorated_rows.pop(child_data.src_node.uid)
            self.displayed_rows.pop(child_data.uid)

        # remove the first child
        self.model.remove(first_child_iter)
        return True

    def remove_all_children(self, parent_iter):
        removed_count = 0
        while self.remove_first_child(parent_iter):
            removed_count += 1
        logger.debug(f'Removed {removed_count} children')
