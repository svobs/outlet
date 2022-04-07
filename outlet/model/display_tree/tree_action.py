from typing import List, Optional

from constants import TreeID
from model.node.node import TNode
from model.node_identifier import GUID
from model.uid import UID


class TreeAction:
    def __init__(self, tree_id: TreeID, action_id: int, target_guid_list: Optional[List[GUID]], target_node_list: Optional[List[TNode]] = None,
                 target_uid: Optional[UID] = None):
        self.tree_id: TreeID = tree_id
        self.action_id: int = action_id
        self.target_guid_list: Optional[List[GUID]] = target_guid_list
        self.target_node_list: Optional[List[TNode]] = target_node_list
        self.target_uid: Optional[UID] = target_uid
