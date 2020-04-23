
class TreePanelController:
    def __init__(self, data_store, display_store, display_meta):
        self.data_store = data_store
        self.display_store = display_store
        self.display_meta = display_meta
        self.tree_view = None
        self.root_dir_panel = None
        self.load_strategy = None
        self.status_bar = None
        self.content_box = None

    def load(self):
        self.load_strategy.populate_root()

