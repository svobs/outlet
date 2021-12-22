# Default version of this file: outlet/template/_menu_item.py
from typing import List
from model.node.node import Node

import logging
logger = logging.getLogger(__name__)

action_id = 101


def is_enabled_for(node_list: List[Node]) -> bool:
    return True


def execute(node_list: List[Node]):
    logger.warning(f'Hello world!')
