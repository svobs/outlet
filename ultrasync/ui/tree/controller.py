
class TreePanelController:
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, parent_win, meta_store, display_store, display_meta):
        self.parent_win = parent_win
        self.meta_store = meta_store
        self.display_store = display_store
        self.display_meta = display_meta
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
        self.display_meta.init()
        self.display_strategy.init()
        self.action_handlers.init()

    def load(self):
        self.display_strategy.populate_root()

    def get_single_selection(self):
        """Assumes that only one node can be selected at a given time"""
        selection = self.tree_view.get_selection()
        model, tree_iter = selection.get_selected_rows()
        if len(tree_iter) == 1:
            return self.display_store.get_node_data(tree_iter)
        elif len(tree_iter) == 0:
            return None
        else:
            raise Exception(f'Selection has more rows than expected: count={len(tree_iter)}')

    def get_multiple_selection(self):
        """Returns a list of the selected items (empty if none)"""
        selection = self.tree_view.get_selection()
        model, tree_iter = selection.get_selected_rows()
        items = []
        while tree_iter is not None:
            item = self.display_store.get_node_data(tree_iter)
            items.append(item)
            tree_iter = self.display_store.model.iter_next(tree_iter)
        return items



