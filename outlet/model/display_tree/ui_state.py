from typing import Optional

from model.node.node import SPIDNodePair


# CLASS DisplayTreeUiState
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class DisplayTreeUiState:
    def __init__(self, tree_id: str, root_sn: SPIDNodePair, root_exists: bool = True, offending_path: Optional[str] = None):
        self.tree_id: str = tree_id
        assert isinstance(root_sn, SPIDNodePair), f'Expected SPIDNodePair but got {type(root_sn)}'
        self.root_sn: SPIDNodePair = root_sn
        """This is needed to clarify the (albeit very rare) case where the root node resolves to multiple paths.
        Each display tree can only have one root path."""
        self.root_exists: bool = root_exists
        self.offending_path: Optional[str] = offending_path
        self.needs_manual_load: bool = False
        """If True, the UI should display a "Load" button in order to kick off the backend data load. 
        If False; the backend will automatically start loading in the background."""
