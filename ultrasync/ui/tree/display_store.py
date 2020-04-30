import logging
from typing import Dict, Union

import gi
from gi.repository.Gtk import TreeIter, TreePath

from model.display_node import DisplayNode
from model.fmeta_tree import FMetaTree
from model.planning_node import PlanningNode

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from model.fmeta import FMeta

logger = logging.getLogger(__name__)


class DisplayStore:
    """(Mostly) encapsulates the nodes inside the TreeView object, which will be a subset of the nodes
    which come from the data store """
    def __init__(self, treeview_meta):
        self.treeview_meta = treeview_meta
        self.model = Gtk.TreeStore()
        self.model.set_column_types(self.treeview_meta.col_types)
        self.selected_rows: Dict[str, DisplayNode] = {}
        self.inconsistent_rows: Dict[str, DisplayNode] = {}

    def get_node_data(self, tree_path: Union[TreeIter, TreePath]):
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The data node (the contents of the hidden "data" column for the given row
        """
        return self.model[tree_path][self.treeview_meta.col_num_data]

    def get_node_name(self, tree_path):
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The value of the 'name' column for the given row
        """
        return self.model[tree_path][self.treeview_meta.col_num_name]

    def is_node_checked(self, tree_path):
        return self.model[tree_path][self.treeview_meta.col_num_checked]

    def is_inconsistent(self, tree_path):
        return self.model[tree_path][self.treeview_meta.col_num_inconsistent]

    def clear_model(self) -> TreeIter:
        self.model.clear()
        return self.model.get_iter_first()

    def set_checked_state(self, tree_iter, is_checked, is_inconsistent):
        assert not (is_checked and is_inconsistent)
        node_data = self.get_node_data(tree_iter)

        if not node_data.has_path():
            # Cannot be checked if no path (LoadingNode, etc.)
            return

        row = self.model[tree_iter]
        row[self.treeview_meta.col_num_checked] = is_checked
        row[self.treeview_meta.col_num_inconsistent] = is_inconsistent

        self.update_checked_state_tracking(node_data, is_checked, is_inconsistent)

    def update_checked_state_tracking(self, node_data, is_checked, is_inconsistent):
        row_id = node_data.display_id.id_string
        if is_checked:
            self.selected_rows[row_id] = node_data
        else:
            if row_id in self.selected_rows: del self.selected_rows[row_id]

        if is_inconsistent:
            self.inconsistent_rows[row_id] = node_data
        else:
            if row_id in self.inconsistent_rows: del self.inconsistent_rows[row_id]

    def on_cell_checkbox_toggled(self, widget, path):
        """Called when checkbox in treeview is toggled"""
        data_node = self.get_node_data(path)
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

                self.update_checked_state_tracking(child_data, child_checked, child_inconsistent)

                child_iter = self.model.iter_next(child_iter)

        # Update all of the node's children to match its check state:
        def update_checked_state(t_iter):
            self.set_checked_state(t_iter, checked_value, False)

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
                self.set_checked_state(tree_iter, is_checked, is_inconsistent)

    # --- Tree searching & iteration (utility functions) --- #

    def recurse_over_tree(self, tree_iter, action_func):
        """
        Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants
        """
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

    def remove_first_child(self, parent_iter):
        first_child_iter = self.model.iter_children(parent_iter)
        if not first_child_iter:
            return False

        if logger.isEnabledFor(logging.DEBUG):
            child_data = self.get_node_data(first_child_iter)
            logger.debug(f'Removing child: {child_data}')
        # remove the first child
        self.model.remove(first_child_iter)
        return True

    def remove_all_children(self, parent_iter):
        removed_count = 0
        while self.remove_first_child(parent_iter):
            removed_count += 1
        logger.debug(f'Removed {removed_count} children')

    def get_checked_rows_as_tree(self):
        """Returns a FMetaTree which contains the FMetas of the rows which are currently
        checked by the user. This will be a subset of the FMetaTree which was used to
        populate this tree."""
        assert self.treeview_meta.editable

        tree_iter = self.model.get_iter_first()
        tree_path = self.model.get_path(tree_iter)
        # TODO; checked rows as
        return self.get_subtree_as_tree(tree_path, include_following_siblings=True, checked_only=True)

    def get_subtree_as_tree(self, tree_path, include_following_siblings=False, checked_only=False):
        """
        FIXME: need to generalize this so it doesn't depend on FMeta!
        Constructs a new FMetaTree out of the data nodes of the subtree referenced
        by tree_path. NOTE: currently the FMeta objects are reused in the new tree,
        for efficiency.
        Args:
            tree_path: root of the subtree, as a GTK3 TreePath
            include_following_siblings: if False, include only the root node and its children
            (filtered by checked state if checked_only is True)
            checked_only: if True, include only rows which are checked
                          if False, include all rows in the subtree
        Returns:
            A new FMetaTree which consists of a subset of the current UI tree
        """
        subtree_root = self.get_node_data(tree_path).full_path
        subtree = FMetaTree(subtree_root)

        def action_func(t_iter):
            if not action_func.checked_only or self.is_node_checked(t_iter):
                data_node = self.get_node_data(t_iter)
                if isinstance(data_node, FMeta) or isinstance(data_node, PlanningNode):
                    subtree.add(data_node)

        action_func.checked_only = checked_only

        if include_following_siblings:
            self.do_for_subtree_and_following_sibling_subtrees(tree_path, action_func)
        else:
            self.do_for_self_and_descendants(tree_path, action_func)

        return subtree
