# Default version of this file: outlet/template/_menu_item.py
from typing import List
from model.node.node import TNode

import logging
logger = logging.getLogger(__name__)

action_id = 101


def get_label(node_list: List[TNode]) -> str:
    return 'Untitled Menu Item'


def is_enabled(node_list: List[TNode]) -> bool:
    return True


def run(node_list: List[TNode]):
    logger.warning(f'Hello world!')
