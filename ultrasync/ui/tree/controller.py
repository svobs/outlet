
class TreePanelController:
    """
    This class is mostly just a place to hold references in memory of all the disparate components
    required to make a tree panel. Hopefully I will think of ways to refine it more in the future.
    """
    def __init__(self, data_store, display_store, display_meta):
        self.data_store = data_store
        self.display_store = display_store
        self.display_meta = display_meta
        self.tree_view = None
        self.root_dir_panel = None
        self.load_strategy = None
        self.status_bar = None
        self.content_box = None
        self.action_bridge = None

    def load(self):
        self.load_strategy.populate_root()

