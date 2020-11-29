
from model.uid import UID


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# CLASS GDriveUser
from util.ensure import ensure_bool


class GDriveUser:
    def __init__(self, display_name, permission_id, email_address, photo_link, is_me: bool = False, user_uid: UID = None):
        # Identifiers:
        self.uid: UID = user_uid
        self.permission_id: str = permission_id

        self.display_name = display_name
        self.email_address = email_address
        self.photo_link = photo_link
        self.is_me = ensure_bool(is_me)

    def update_from(self, other):
        assert isinstance(other, GDriveUser)
        self.display_name = other.display_name
        self.email_address = other.email_address
        self.photo_link = other.photo_link
        self.is_me = other.is_me


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# CLASS MimeType
class MimeType:
    def __init__(self, uid: UID, type_string: str):
        self.uid: UID = uid
        self.type_string: str = type_string
