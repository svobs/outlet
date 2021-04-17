import logging
from typing import Callable

from constants import TreeDisplayMode, TreeID
from model.node.container_node import CategoryNode
from model.node.node import Node, SPIDNodePair
from signal_constants import Signal
from util.ensure import ensure_bool, ensure_int
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class TreeViewMeta(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeViewMeta
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def but_with_checkboxes(self, has_checkboxes: bool, tree_id: TreeID):
        """Return an exact duplicate of this class instance, but with has_checkboxes set to the desired value"""
        new_inst: TreeViewMeta = TreeViewMeta(backend=self.backend, tree_id=tree_id, can_modify_tree=self.can_modify_tree,
                                              has_checkboxes=has_checkboxes, can_change_root=self.can_change_root,
                                              tree_display_mode=self.tree_display_mode, lazy_load=self.lazy_load, selection_mode=self.selection_mode,
                                              is_display_persisted=self.is_display_persisted, is_ignored_func=self.is_ignored_func)
        return new_inst

    def __init__(self, backend, tree_id: TreeID, can_modify_tree: bool, has_checkboxes: bool, can_change_root: bool,
                 tree_display_mode: TreeDisplayMode, lazy_load: bool, selection_mode, is_display_persisted: bool, is_ignored_func):
        HasLifecycle.__init__(self)
        self.backend = backend
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
        self.use_dir_tree: bool = ensure_bool(backend.get_config('display.treeview.use_dir_tree'))

        self.show_modify_ts_col: bool = ensure_bool(backend.get_config('display.treeview.show_modify_ts_col'))
        self.show_change_ts_col: bool = ensure_bool(backend.get_config('display.treeview.show_change_ts_col'))
        self.show_etc_col: bool = ensure_bool(backend.get_config('display.treeview.show_etc_col'))

        self.datetime_format = backend.get_config('display.treeview.datetime_format')
        self.extra_indent: int = ensure_int(backend.get_config('display.treeview.extra_indent'))
        self.row_height: int = ensure_int(backend.get_config('display.treeview.row_height'))

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

    def start(self):
        logger.debug(f'[{self.tree_id}] TreeViewMeta init is_persisted={self.is_display_persisted}')
        HasLifecycle.start(self)

        # TODO: remove this listener entirely, after verifying that DisplayMutator covers Category nodes.
        # Hook up persistence of expanded state (if configured):
        if self.is_display_persisted:
            self.connect_dispatch_listener(signal=Signal.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled)

    def shutdown(self):
        HasLifecycle.shutdown(self)

    def _on_node_expansion_toggled(self, sender: str, parent_iter, parent_path, sn: SPIDNodePair, is_expanded: bool):
        if sender != self.tree_id:
            return

        if type(sn.node) == CategoryNode:
            logger.debug(f'[{self.tree_id}] Detected node expansion toggle: {sn.spid.op_type.name} = {is_expanded}')
            cfg_path = f'ui_state.{self.tree_id}.expanded_state.{sn.spid.op_type.name}'
            self.backend.put_config(cfg_path, is_expanded)
        # Allow other listeners to handle this also:
        return False

    def is_category_node_expanded(self, node: Node):
        if self.is_display_persisted:
            assert isinstance(node, CategoryNode)
            cfg_path = f'ui_state.{self.tree_id}.expanded_state.{node.op_type.name}'
            return ensure_bool(self.backend.get_config(cfg_path, True))

        # Default if no config:
        return True
