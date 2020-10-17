
from model.uid import UID
from model.node_identifier import ensure_bool


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# CLASS GDriveUser
class GDriveUser:
    def __init__(self, display_name, permission_id, email_address, photo_link, is_me: bool = False, user_uid: UID = None):
        self.uid = user_uid
        self.display_name = display_name
        self.permission_id = permission_id
        self.email_address = email_address
        self.photo_link = photo_link
        self.is_me = ensure_bool(is_me)


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# CLASS MimeType
class MimeType:
    def __init__(self, uid: UID, type_string: str):
        self.uid: UID = uid
        self.type_string: str = type_string
