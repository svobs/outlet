from typing import List, Optional

from constants import MenuItemType


class ContextMenuItem:
    def __init__(self, item_type: MenuItemType, title: Optional[str], action_id: int):
        self.item_type: MenuItemType = item_type
        self.title: str = title
        self.action_id: int = action_id
        self.submenu_item_list: List = []

    def add_submenu_item(self, item):
        self.submenu_item_list.append(item)
