from typing import Optional

from model.node_identifier import SinglePathNodeIdentifier


#    CLASS RootPathMeta
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class RootPathMeta:
    def __init__(self, new_root: SinglePathNodeIdentifier, is_found: bool, offending_path: str = None):
        assert isinstance(new_root, SinglePathNodeIdentifier), f'Wrong instance: {type(new_root)}: {new_root}'
        if not new_root or not new_root.get_path_list():
            raise RuntimeError(f'Root path cannot be empty! (root={new_root})')
        self.root: SinglePathNodeIdentifier = new_root
        self.is_found: bool = is_found
        """False if root not found"""
        self.offending_path: Optional[str] = offending_path
        """Only present in some cases where root not found"""

    def __repr__(self):
        return f'RootPathMeta(new_root={self.root}, is_found={self.is_found}, offending_path={self.offending_path})'

    def __eq__(self, other):
        return self.root == other.root and self.is_found == other.is_found and self.offending_path == other.offending_path

    def __ne__(self, other):
        return not self.__eq__(other)

