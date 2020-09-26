import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from model.node.gdrive_node import GDriveNode

logger = logging.getLogger(__name__)

# CLASS GDriveChange
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveChange:
    def __init__(self, change_ts, goog_id: str):
        self.change_ts = change_ts
        self.goog_id = goog_id

    @classmethod
    def is_removed(cls):
        return False


# CLASS GDriveRM
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveRM(GDriveChange):
    def __init__(self, change_ts, goog_id: str):
        super().__init__(change_ts, goog_id)

    @classmethod
    def is_removed(cls):
        return True


# CLASS GDriveNodeChange
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveNodeChange(GDriveChange):
    def __init__(self, change_ts, goog_id: str, node: GDriveNode):
        super().__init__(change_ts, goog_id)
        self.node = node


# CLASS GDriveChangeList
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveChangeList:
    def __init__(self, change_list: List[GDriveChange] = None, new_start_token: str = None):
        self.change_list: List[GDriveChange] = change_list
        if not self.change_list:
            self.change_list = []
        self.new_start_token: str = new_start_token


# ABSTRACT CLASS GDriveChangeObserver
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class GDriveChangeObserver(ABC):
    """Observer interface, to be implemented with various strategies for processing downloaded Google Drive query results"""

    def __init__(self):
        self.new_start_token: Optional[str] = None

    @abstractmethod
    def change_received(self, change: GDriveChange, item):
        pass

    @abstractmethod
    def end_of_page(self, next_page_token: str):
        pass


# CLASS PagePersistingChangeObserver
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class PagePersistingChangeObserver(GDriveChangeObserver):
    """Observer interface, to be implemented with various strategies for processing downloaded Google Drive query results"""

    def __init__(self, application):
        super().__init__()
        self.application = application
        self.change_list: List[GDriveChange] = []

    def change_received(self, change: GDriveChange, item):
        self.change_list.append(change)

    def end_of_page(self, next_page_token: str):
        # TODO: add reduce()
        for change in self.change_list:
            if change.is_removed():
                node = self.application.cache_manager.get_node_for_goog_id(change.goog_id)
                if node:
                    self.application.cache_manager.remove_node(node, to_trash=False)
                else:
                    logger.debug(f'No node found in cache for goog_id: "{change.goog_id}"')
            else:
                assert isinstance(change, GDriveNodeChange)
                self.application.cache_manager.add_or_update_node(change.node)


