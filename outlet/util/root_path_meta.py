from typing import Optional

from model.node_identifier import SinglePathNodeIdentifier


class RootPathMeta:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RootPathMeta
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, new_root_spid: SinglePathNodeIdentifier, root_exists: bool, offending_path: str = None):
        assert isinstance(new_root_spid, SinglePathNodeIdentifier), f'Wrong instance: {type(new_root_spid)}: {new_root_spid}'
        if not new_root_spid or not new_root_spid.get_path_list():
            raise RuntimeError(f'Root path cannot be empty! (root_spid={new_root_spid})')

        self.root_spid: SinglePathNodeIdentifier = new_root_spid
        self.root_exists: bool = root_exists
        """False if root not found"""
        self.offending_path: Optional[str] = offending_path
        """Only present in some cases where root not found"""

    def __repr__(self):
        return f'RootPathMeta(new_root={self.root_spid}, root_exists={self.root_exists}, offending_path={self.offending_path})'

    def __eq__(self, other):
        return self.root_spid == other.root_spid and self.root_exists == other.root_exists and self.offending_path == other.offending_path

    def __ne__(self, other):
        return not self.__eq__(other)

