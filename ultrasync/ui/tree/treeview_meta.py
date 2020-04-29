from pydispatch import dispatcher

from model.display_node import CategoryNode
from ui import actions


class TreeViewMeta:
    def __init__(self, config, tree_id, editable, selection_mode, is_display_persisted, is_ignored_func=None):
        self.config = config
        self.selection_mode = selection_mode
        self.tree_id = tree_id
        """If false, disable actions in UI"""
        self.editable = editable
        """If true, load and save aesthetic things like expanded state of some nodes"""
        self.is_display_persisted = is_display_persisted
        # This is a function pointer which accepts a data node arg and returns true if it is considered "ignored":
        self.is_ignored_func = is_ignored_func

        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = config.get('display.diff_tree.use_dir_tree')
        self.show_change_ts = config.get('display.diff_tree.show_change_ts')
        self.datetime_format = config.get('display.diff_tree.datetime_format')
        self.extra_indent = config.get('display.diff_tree.extra_indent')
        self.row_height = config.get('display.diff_tree.row_height')

        col_count = 0
        self.col_types = []
        self.col_names = []
        if self.editable:
            self.col_num_checked = col_count
            self.col_names.append('Checked')
            self.col_types.append(bool)
            col_count += 1

            self.col_num_inconsistent = col_count
            self.col_names.append('Inconsistent')
            self.col_types.append(bool)
            col_count += 1
        self.col_num_icon = col_count
        self.col_names.append('Icon')
        self.col_types.append(str)
        col_count += 1

        self.col_num_name = col_count
        self.col_names.append('Name')
        self.col_types.append(str)
        col_count += 1

        if not self.use_dir_tree:
            self.col_num_directory = col_count
            self.col_names.append('Directory')
            self.col_types.append(str)
            col_count += 1

        self.col_num_size = col_count
        self.col_names.append('Size')
        self.col_types.append(str)
        col_count += 1

        self.col_num_modification_ts = col_count
        self.col_names.append('Modification Time')
        self.col_types.append(str)
        col_count += 1

        if self.show_change_ts:
            self.col_num_change_ts = col_count
            self.col_names.append('Meta Change Time')
            self.col_types.append(str)
            col_count += 1

        self.col_num_data = col_count
        self.col_names.append('Data')
        self.col_types.append(object)
        col_count += 1

    def init(self):
        # Hook up persistence of expanded state (if configured):
        if self.is_display_persisted:
            dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.tree_id)

    def _on_node_expansion_toggled(self, sender, parent_iter, node_data, is_expanded, expand_all=False):
        if type(node_data) == CategoryNode:
            if self.is_ignored_func and self.is_ignored_func(node_data):
                # Do not expand if ignored:
                return
            cfg_path = f'transient.{self.tree_id}.expanded_state.{node_data.category.name}'
            self.config.write(cfg_path, is_expanded)
        # Allow other listeners to handle this also:
        return False

    def is_category_node_expanded(self, node):
        if self.is_ignored_func and self.is_ignored_func(node):
            # Do not expand if ignored:
            return False

        if self.is_display_persisted:
            cfg_path = f'transient.{self.tree_id}.expanded_state.{node.category.name}'
            return self.config.get(cfg_path, True)

        # Default if no config:
        return True
