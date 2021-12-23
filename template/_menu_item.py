# Default version of this file: outlet/template/_menu_item.py
from typing import List
from model.node.node import Node

import logging
logger = logging.getLogger(__name__)

action_id = 101


def get_label(node_list: List[Node]) -> str:
    return 'Untitled Menu Item'


def is_enabled(node_list: List[Node]) -> bool:
    return True


def run(node_list: List[Node]):
    logger.warning(f'Hello world!')
