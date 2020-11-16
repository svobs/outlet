from typing import Callable, Optional

from pydispatch import dispatcher
import logging

from pydispatch.errors import DispatcherKeyError

from app_config import AppConfig
from constants import TreeDisplayMode
from model.node.container_node import CategoryNode
from model.node.node import Node
from model.node_identifier import ensure_bool, ensure_int
from ui import actions
from ui.tree.filter_criteria import BoolOption, FilterCriteria

logger = logging.getLogger(__name__)


# CLASS TreeViewMeta
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeViewMeta:
    def but_with_checkboxes(self, checkboxes_visible: bool):
        """Return an exact duplicate of this class instance, but with has_checkboxes set to the desired value"""
        new_inst: TreeViewMeta = TreeViewMeta(config=self.config, tree_id=self.tree_id, can_modify_tree=self.can_modify_tree,
                                              has_checkboxes=checkboxes_visible, can_change_root=self.can_change_root,
                                              tree_display_mode=self.tree_display_mode, lazy_load=self.lazy_load, selection_mode=self.selection_mode,
                                              is_display_persisted=self.is_display_persisted, is_ignored_func=self.is_ignored_func)
        new_inst.filter_criteria = self.filter_criteria
        return new_inst

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
        self.is_ignored_func: Callable[[Node], bool] = is_ignored_func
        """This is a function pointer which accepts a data node arg and returns true if it is considered ignored"""

        self.tree_display_mode: TreeDisplayMode = tree_display_mode
        self.lazy_load: bool = lazy_load

        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = config.get('display.diff_tree.use_dir_tree')

        self.show_modify_ts_col: bool = config.get('display.diff_tree.show_modify_ts_col')
        self.show_change_ts_col: bool = config.get('display.diff_tree.show_change_ts_col')
        self.show_etc_col: bool = config.get('display.diff_tree.show_etc_col')

        self.datetime_format = config.get('display.diff_tree.datetime_format')
        self.extra_indent: int = config.get('display.diff_tree.extra_indent')
        self.row_height: int = config.get('display.diff_tree.row_height')

        self.filter_criteria: Optional[FilterCriteria] = None

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
        logger.debug(f'[{self.tree_id}] TreeViewMeta init is_persisted={self.is_display_persisted}')
        # Hook up persistence of expanded state (if configured):
        if self.is_display_persisted:
            dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.tree_id)

    def destroy(self):
        if self.is_display_persisted:
            try:
                dispatcher.disconnect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.tree_id)
            except DispatcherKeyError:
                pass

    def read_filter_criteria_from_config(self):
        logger.debug(f'[{self.tree_id}] Reading FilterCriteria from config')
        self.filter_criteria = FilterCriteria.read_filter_criteria_from_config(self.config, self.tree_id)

    def write_filter_criteria_to_config(self):
        if self.filter_criteria:
            logger.debug(f'[{self.tree_id}] Writing FilterCriteria to config')
            self.filter_criteria.write_filter_criteria_to_config(config=self.config, tree_id=self.tree_id)
        else:
            logger.debug(f'[{self.tree_id}] No FilterCriteria to write')

    def _on_node_expansion_toggled(self, sender: str, parent_iter, parent_path, node: Node, is_expanded: bool):
        if type(node) == CategoryNode:
            assert isinstance(node, CategoryNode)
            logger.debug(f'[{self.tree_id}] Detected node expansion toggle: {node.op_type} = {is_expanded}')
            cfg_path = f'ui_state.{self.tree_id}.expanded_state.{node.op_type.name}'
            self.config.write(cfg_path, is_expanded)
        # Allow other listeners to handle this also:
        return False

    def is_category_node_expanded(self, node: Node):
        if self.is_display_persisted:
            assert isinstance(node, CategoryNode)
            cfg_path = f'ui_state.{self.tree_id}.expanded_state.{node.op_type.name}'
            return self.config.get(cfg_path, True)

        # Default if no config:
        return True
