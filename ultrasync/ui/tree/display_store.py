import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf


class DisplayStore:
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

