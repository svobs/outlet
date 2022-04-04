from typing import List, Optional

from constants import ActionID, MenuItemType
from model.node_identifier import GUID
from model.uid import UID


class ContextMenuItem:
    def __init__(self, item_type: MenuItemType, title: Optional[str], action_id: int):
        self.item_type: MenuItemType = item_type
        self.title: str = title
        self.action_id: int = action_id
        # Only used for menu items which apply to a subset, but not the entirety, of the selected nodes:
        self.target_guid_list: List[GUID] = []
        self.submenu_item_list: List = []
        self.target_uid: Optional[UID] = None  # only used by certain actions, if at all

    def add_submenu_item(self, item):
        self.submenu_item_list.append(item)

    @staticmethod
    def make_italic_disabled(title: str):
        return ContextMenuItem(item_type=MenuItemType.ITALIC_DISABLED, title=title, action_id=ActionID.NO_ACTION)

    @staticmethod
    def make_separator():
        return ContextMenuItem(item_type=MenuItemType.SEPARATOR, title="", action_id=ActionID.NO_ACTION)
