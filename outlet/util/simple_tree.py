from typing import Dict, List

from model.node.node import Node
from model.uid import UID


class SimpleTree:
    def __init__(self):
        self._parent_child_list_dict: Dict[UID, List[Node]] = {}


