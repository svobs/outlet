import logging

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

logger = logging.getLogger(__name__)


class DisplayStore:
    """(Mostly) encapsulates the nodes inside the TreeView object, which will be a subset of the nodes
    which """
    def __init__(self, display_meta):
        self.display_meta = display_meta
        self.model = Gtk.TreeStore()
        self.model.set_column_types(self.display_meta.col_types)

    def get_node_data(self, tree_path):
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The data node (the contents of the hidden "data" column for the given row
        """
        return self.model[tree_path][self.display_meta.col_num_data]

    def get_node_name(self, tree_path):
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The value of the 'name' column for the given row
        """
        return self.model[tree_path][self.display_meta.col_num_name]

    def is_node_checked(self, tree_path):
        return self.model[tree_path][self.display_meta.col_num_checked]

    def set_checked(self, tree_path, checked_value):
        self.model[tree_path][self.display_meta.col_num_checked] = checked_value

    def is_inconsistent(self, tree_path):
        return self.model[tree_path][self.display_meta.col_num_inconsistent]

    def set_inconsistent(self, tree_path, inconsistent_value):
        self.model[tree_path][self.display_meta.col_num_inconsistent] = inconsistent_value

    def on_cell_checkbox_toggled(self, widget, path):
        """Called when checkbox in treeview is toggled"""
        data_node = self.get_node_data(path)
        if self.display_meta.is_ignored_func:
            if self.display_meta.is_ignored_func(data_node):
                logger.debug('Disallowing checkbox toggle because node is in IGNORED category')
                return
        # DOC: model[path][column] = not model[path][column]
        checked_value = not self.is_node_checked(path)
        logger.debug(f'Toggled {checked_value}: {self.get_node_name(path)}')

        # Update all of the node's children change to match its check state:
        def update_checked_state(t_iter):
            self.set_checked(t_iter, checked_value)
            self.set_inconsistent(t_iter, False)

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
                self.set_inconsistent(tree_iter, has_inconsistent or (has_checked and has_unchecked))
                self.set_checked(tree_iter, has_checked and not has_unchecked and not has_inconsistent)

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
