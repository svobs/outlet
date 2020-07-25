from model.node_identifier import NodeIdentifier


#    CLASS GDriveItemNotFoundError
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveItemNotFoundError(RuntimeError):
    def __init__(self, node_identifier: NodeIdentifier, offending_path: str, msg: str = None):
        if msg is None:
            # Set some default useful error message
            msg = f'Google Drive object not found: {offending_path}'
        super(GDriveItemNotFoundError, self).__init__(msg)
        self.node_identifier = node_identifier
        self.offending_path = offending_path


#    CLASS CacheNotLoadedError
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class CacheNotLoadedError(RuntimeError):
    def __init__(self, msg: str = None):
        if msg is None:
            # Set some default useful error message
            msg = f'Cache not loaded!'
        super(CacheNotLoadedError, self).__init__(msg)
