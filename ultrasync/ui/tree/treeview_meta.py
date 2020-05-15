from pydispatch import dispatcher

from app_config import AppConfig
from constants import TreeDisplayMode
from model.display_node import CategoryNode
from ui import actions


class TreeViewMeta:
    def but_with_checkboxes(self):
        return TreeViewMeta(config=self.config, tree_id=self.tree_id, can_modify_tree=self.can_modify_tree, has_checkboxes=True, can_change_root=self.can_change_root,
                            tree_display_mode=self.tree_display_mode, lazy_load=self.lazy_load, selection_mode=self.selection_mode,
                            is_display_persisted=self.is_display_persisted, is_ignored_func=self.is_ignored_func)

    def __init__(self, config: AppConfig, tree_id: str, can_modify_tree: bool, has_checkboxes: bool, can_change_root: bool,
                 tree_display_mode: TreeDisplayMode, lazy_load: bool, selection_mode, is_display_persisted: bool, is_ignored_func):
        self.config = config
        self.selection_mode = selection_mode
        self.tree_id = tree_id

        self.can_modify_tree = can_modify_tree
        """If true, can delete nodes, rename them, etc"""
        self.has_checkboxes: bool = has_checkboxes
        self.can_change_root: bool = can_change_root
        """If false, make the root path display panel read-only"""
        self.is_display_persisted = is_display_persisted
        """If true, load and save aesthetic things like expanded state of some nodes"""
        self.is_ignored_func = is_ignored_func
        """This is a function pointer which accepts a data node arg and returns true if it is considered ignored"""

        self.tree_display_mode: TreeDisplayMode = tree_display_mode
        self.lazy_load: bool = lazy_load
        """If true, display category trees for items which are not Category.NA. If false, show all items and
        do not use category nodes."""

        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = config.get('display.diff_tree.use_dir_tree')

        self.show_modify_ts_col = config.get('display.diff_tree.show_modify_ts_col')
        self.show_change_ts_col = config.get('display.diff_tree.show_change_ts_col')
        self.show_etc_col = config.get('display.diff_tree.show_etc_col')

        self.datetime_format = config.get('display.diff_tree.datetime_format')
        self.extra_indent = config.get('display.diff_tree.extra_indent')
        self.row_height = config.get('display.diff_tree.row_height')

        # Search for "TREE_VIEW_COLUMNS":

        # model and treeview have different ways of counting the columns:
        col_count_model = 0
        col_count_view = 0
        self.col_types = []
        self.col_names = []
        if self.has_checkboxes:
            self.col_num_checked = col_count_model
            self.col_names.append('Checked')
            self.col_types.append(bool)
            col_count_model += 1

            self.col_num_inconsistent = col_count_model
            self.col_names.append('Inconsistent')
            self.col_types.append(bool)
            col_count_model += 1
        self.col_num_icon = col_count_model
        self.col_names.append('Icon')
        self.col_types.append(str)
        col_count_model += 1

        self.col_num_name = col_count_model
        self.col_names.append('Name')
        self.col_types.append(str)
        col_count_model += 1
        self.col_num_name_view = col_count_view
        col_count_view += 1

        if not self.use_dir_tree:
            self.col_num_directory = col_count_model
            self.col_names.append('Directory')
            self.col_types.append(str)
            col_count_model += 1
            self.col_num_directory_view = col_count_view
            col_count_view += 1

        self.col_num_size = col_count_model
        self.col_names.append('Size')
        self.col_types.append(str)
        col_count_model += 1
        self.col_num_size_view = col_count_view
        col_count_view += 1

        self.col_num_etc = col_count_model
        self.col_names.append('Etc')
        self.col_types.append(str)
        col_count_model += 1
        self.col_num_etc_view = col_count_view
        col_count_view += 1

        self.col_num_modification_ts = col_count_model
        self.col_names.append('Modification Time')
        self.col_types.append(str)
        col_count_model += 1
        self.col_num_modify_ts_view = col_count_view
        col_count_view += 1

        self.col_num_change_ts = col_count_model
        self.col_names.append('Meta Change Time')
        self.col_types.append(str)
        col_count_model += 1
        self.col_num_change_ts_view = col_count_view
        col_count_view += 1

        self.col_num_data = col_count_model
        self.col_names.append('Data')
        self.col_types.append(object)
        col_count_model += 1

    def init(self):
        # Hook up persistence of expanded state (if configured):
        if self.is_display_persisted:
            dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.tree_id)

    def _on_node_expansion_toggled(self, sender, parent_iter, node_data, is_expanded, expand_all=False):
        if type(node_data) == CategoryNode:
            if self.is_ignored_func and self.is_ignored_func(node_data):
                # Do not expand if ignored:
                return False
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
