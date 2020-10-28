

#    CLASS GDriveItemNotFoundError
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class GDriveItemNotFoundError(RuntimeError):
    def __init__(self, node_identifier, offending_path: str, msg: str = None):
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


#    CLASS IdenticalFileExistsError
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class IdenticalFileExistsError(Exception):
    def __init__(self, *args, **kwargs):
        pass


# TODO: make into decorator
class InvalidOperationError(RuntimeError):
    def __init__(self, operation_name: str = None):
        if not operation_name:
            msg = f'Invalid operation!'
        else:
            msg = f'Invalid operation: "{operation_name}"'
        super(InvalidOperationError, self).__init__(msg)

