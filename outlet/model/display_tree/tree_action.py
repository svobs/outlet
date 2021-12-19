from typing import List, Optional

from constants import ActionID, TreeID
from model.node.node import Node
from model.node_identifier import GUID


class TreeAction:
    def __init__(self, tree_id: TreeID, action_id: ActionID, target_guid_list: Optional[List[GUID]], target_node_list: Optional[List[Node]] = None):
        self.tree_id: TreeID = tree_id
        self.action_id: ActionID = action_id
        self.target_guid_list: Optional[List[GUID]] = target_guid_list
        self.target_node_list: Optional[List[Node]] = target_node_list
