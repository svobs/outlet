
class UID(int):
    def __new__(cls, val, *args, **kwargs):
        return super(UID, cls).__new__(cls, val)


class AUID(int):
    def __new__(cls, val, *args, **kwargs):
        return super(AUID, cls).__new__(cls, val)
