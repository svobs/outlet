
class TreePanelController:
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, parent_win, data_store, display_store, display_meta):
        self.parent_win = parent_win
        self.data_store = data_store
        self.display_store = display_store
        self.display_meta = display_meta
        self.tree_view = None
        self.root_dir_panel = None
        self.display_strategy = None
        self.status_bar = None
        self.content_box = None
        self.action_handlers = None

    @property
    def tree_id(self):
        return self.data_store.tree_id

    def init(self):
        """Should be called after all controller components have been wired together"""
        self.display_strategy.init()
        self.action_handlers.init()

    def load(self):
        self.display_strategy.populate_root()

