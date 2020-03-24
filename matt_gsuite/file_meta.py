class FileEntry:
    def __init__(self, signature, length, file_path):
        self.signature = signature
        self.length = length
        self.file_path = file_path
        self.deleted = False


class FilesMeta:
    def __init__(self):
        self.sig_dict = {}
        self.path_dict = {}
