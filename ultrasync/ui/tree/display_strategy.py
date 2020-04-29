from abc import ABC, abstractmethod

from gi.repository.Gtk import TreeIter

from model.display_node import EmptyNode, LoadingNode


class DisplayStrategy(ABC):
    def __init__(self, controller=None):
        self.con = controller
        self.use_empty_nodes = True

    @abstractmethod
    def populate_root(self):
        """Draws from the undelying data store as needed, to populate the display store."""
        pass

    @abstractmethod
    def init(self):
        """Do post-wiring stuff like connect listeners."""
        self.use_empty_nodes = self.con.config.get('display.diff_tree.use_empty_nodes')

    def append_dir_node_and_empty_child(self, tree_iter, node_data):
        dir_node_iter = self.append_dir_node(tree_iter, node_data)
        self._append_loading_child(dir_node_iter)
        return dir_node_iter

    def _append_empty_child(self, parent_node_iter):
        row_values = []
        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(None)  # Icon
        row_values.append('(empty)')  # Name
        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(EmptyNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    def _append_loading_child(self, parent_node_iter):
        row_values = []
        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append('folder')  # Icon
        row_values.append('Loading...')  # Name
        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        row_values.append(None)  # Size
        row_values.append(None)  # Modify Date
        row_values.append(None)  # Created Date
        row_values.append(LoadingNode())

        return self.con.display_store.model.append(parent_node_iter, row_values)

    @abstractmethod
    def append_dir_node(self, tree_iter, node_data) -> TreeIter:
        pass
