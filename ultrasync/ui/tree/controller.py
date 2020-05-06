from typing import List
import logging

from stopwatch import Stopwatch

from model.display_node import DisplayNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)


class TreePanelController:
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, parent_win, meta_store, display_store, treeview_meta):
        self.parent_win = parent_win
        self.meta_store = meta_store
        self.display_store = display_store
        self.treeview_meta = treeview_meta
        self.tree_view = None
        self.root_dir_panel = None
        self.display_strategy = None
        self.status_bar = None
        self.content_box = None
        self.action_handlers = None

    @property
    def config(self):
        """Convenience method. Retreives the tree_id from the parent_win"""
        return self.parent_win.config

    @property
    def tree_id(self):
        """Convenience method. Retreives the tree_id from the metastore"""
        return self.meta_store.tree_id

    def init(self):
        """Should be called after all controller components have been wired together"""
        self.treeview_meta.init()
        self.display_strategy.init()
        self.action_handlers.init()

    def load(self):
        self.display_strategy.populate_root()

    def get_single_selection(self):
        """Assumes that only one node can be selected at a given time"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        if len(tree_paths) == 1:
            return self.display_store.get_node_data(tree_paths)
        elif len(tree_paths) == 0:
            return None
        else:
            raise Exception(f'Selection has more rows than expected: count={len(tree_paths)}')

    def get_multiple_selection(self) -> List[DisplayNode]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        items = []
        for tree_path in tree_paths:
            item = self.display_store.get_node_data(tree_path)
            items.append(item)
        return items

    def get_checked_rows_as_tree(self) -> SubtreeSnapshot:
        timer = Stopwatch()
        subtree: SubtreeSnapshot = self.display_strategy.get_checked_rows_as_tree()
        logger.debug(f'Retreived checked rows in {timer}: {subtree.get_summary()}')

        return subtree

