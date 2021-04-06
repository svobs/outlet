import logging
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from error import NodeAlreadyPresentError, NodeNotPresentError
from model.node.node import BaseNode
from model.uid import UID
from util.base_tree import BaseTree

logger = logging.getLogger(__name__)


class SimpleTree(BaseTree):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SimpleTree
    
    Originally based on a simplifications of treelib.Tree.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, extract_identifier_func: Callable = None):
        super().__init__(extract_identifier_func)
        self._node_dict: Dict[Any, BaseNode] = {}
        self._parent_child_list_dict: Dict[UID, List[BaseNode]] = {}
        self._child_parent_dict: Dict[UID, BaseNode] = {}
        """Really need this to efficiently execute remove_node()"""
        self._root_node: Optional[BaseNode] = None

    def __len__(self):
        return len(self._node_dict)

    def get_node_for_uid(self, uid: Any) -> Optional[BaseNode]:
        return self._node_dict.get(uid, None)

    def get_root_node(self) -> Optional[BaseNode]:
        return self._root_node

    def add_node(self, node: BaseNode, parent: Optional[BaseNode]):
        if not isinstance(node, BaseNode):
            raise RuntimeError(f'Cannot add node: it must be an instance of BaseNode (found {type(node)}): {node}')

        node_identifier = self.extract_identifier(node)

        if node_identifier in self._node_dict:
            raise NodeAlreadyPresentError(f'Cannot add node: it is already present in this tree: {node}')

        if not parent:
            if self._root_node:
                raise NodeAlreadyPresentError(f'Cannot add node as root: this tree already has a root node!')
            else:
                self._root_node = node
        elif self.extract_identifier(parent) not in self._node_dict:
            raise NodeNotPresentError(f'Cannot add node ({node_identifier}): parent "{self.extract_identifier(parent)}" not found in tree!')
        else:
            parent_identifier = self.extract_identifier(parent)
            child_list: List[BaseNode] = self._parent_child_list_dict.get(parent_identifier, [])
            if not child_list:
                self._parent_child_list_dict[parent_identifier] = child_list
            child_list.append(node)
            self._child_parent_dict[node_identifier] = parent

        self._node_dict[node_identifier] = node

    def _remove_node_with_identifier(self, node_list: List[BaseNode], uid: Any):
        for node in node_list:
            if self.extract_identifier(node) == uid:
                node_list.remove(node)
                return node
        return None

    def remove_node(self, uid: UID) -> int:
        if self._root_node and self.extract_identifier(self._root_node) == uid:
            self._root_node = None
            count_removed: int = len(self._node_dict)
            self._node_dict.clear()
            self._parent_child_list_dict.clear()
            self._child_parent_dict.clear()
            return count_removed

        node = self._node_dict.get(uid, None)
        if not node:
            raise NodeNotPresentError(f'Cannot remove node: identifier "{uid}" not found in tree!')

        nid_queue: Deque[Any] = deque()
        nid_queue.append(uid)

        # Remove target node from parent's child list
        parent = self.get_parent(uid)
        if parent:
            child_list: List[BaseNode] = self._parent_child_list_dict.get(self.extract_identifier(parent), None)
            if child_list:
                self._remove_node_with_identifier(child_list, uid)

        # Now loop and remove target node and all its descendants:
        count_removed = 0
        while len(nid_queue) > 0:
            count_removed += 1
            identifier = nid_queue.popleft()
            removed_node = self._node_dict.pop(identifier, None)
            if not removed_node:
                raise NodeNotPresentError(f'Cannot remove node: it is not present in tree: {identifier}')

            child_list: List[BaseNode] = self._parent_child_list_dict.pop(identifier, None)
            if child_list:
                for child in child_list:
                    nid_queue.append(child.identifier)

        return count_removed

    def paste(self, parent_uid: UID, new_tree):
        root_to_insert = new_tree.get_root_node()
        if not root_to_insert:
            return

        if not parent_uid:
            raise RuntimeError('uid not provided')

        parent_node = self.get_node_for_uid(parent_uid)
        if not parent_node:
            raise NodeNotPresentError(f'Node with uid "{parent_uid}" is not in this tree')

        node_queue: Deque[Tuple[BaseNode, BaseNode]] = deque()
        node_queue.append((root_to_insert, parent_node))

        count_added = 0
        while len(node_queue) > 0:
            node, parent = node_queue.popleft()
            self.add_node(node, parent)
            count_added += 1
            for child in new_tree.get_child_list_for_uid(self.extract_identifier(node)):
                node_queue.append((child, node))
        return count_added

    def get_child_list(self, node) -> List:
        return self.get_child_list_for_uid(node.uid)

    def get_child_list_for_uid(self, parent_uid: UID) -> List[BaseNode]:
        if parent_uid not in self._node_dict:
            raise NodeNotPresentError(f'Cannot get children: parent "{parent_uid}" is not in the tree!')
        return self._parent_child_list_dict.get(parent_uid, [])

    def get_parent(self, child_nid: Any) -> Optional[BaseNode]:
        return self._child_parent_dict.get(child_nid, None)

    def contains(self, uid: Any) -> bool:
        return uid in self._node_dict

    # The following garbage was copied from treelib.Tree. Should clean this up if there's time
    def show(self, uid=None, level=0, filter_func=None,
             key=None, reverse=False, line_type='ascii-ex', show_identifier=False):
        """
        Print the tree structure in hierarchy style.

        You have three ways to output your tree data, i.e., stdout with ``show()``,
        plain text file with ``save2file()``, and json string with ``to_json()``. The
        former two use the same backend to generate a string of tree structure in a
        text graph.

        * Version >= 1.2.7a*: you can also specify the ``line_type`` parameter, such as 'ascii' (default), 'ascii-ex', 'ascii-exr', 'ascii-em', 'ascii-emv', 'ascii-emh') to the change graphical form.

        :param uid: the reference node to start expanding.
        :param level: the node level in the tree (root as level 0).
        :param filter_func: the function of one variable to act on the :class:`Node` object.
            When this parameter is specified, the traversing will not continue to following
            children of node whose condition does not pass the filter.
        :param key: the ``key`` param for sorting :class:`Node` objects in the same level.
        :param reverse: the ``reverse`` param for sorting :class:`Node` objects in the same level.
        :param line_type:
        :param show_identifier: whether to print the identifier also.
        :return: None
        """
        self._reader = ""

        def write(line):
            self._reader += line.decode('utf-8') + "\n"

        try:
            self.__print_backend(uid, level, filter_func,
                                 key, reverse, line_type, show_identifier, func=write)
        except NodeNotPresentError:
            print('Tree is empty')

        return self._reader

    def __print_backend(self, uid=None, level=0, filter_func=None,
                        key=None, reverse=False, line_type='ascii-ex',
                        show_identifier=False, func=print):
        """
        Another implementation of printing tree using Stack
        Print tree structure in hierarchy style.

        For example:

        .. code-block:: bash

            Root
            |___ C01
            |    |___ C11
            |         |___ C111
            |         |___ C112
            |___ C02
            |___ C03
            |    |___ C31

        A more elegant way to achieve this function using Stack
        structure, for constructing the Nodes Stack push and pop nodes
        with additional level info.

        UPDATE: the @key @reverse is present to sort node at each
        level.
        """
        # Factory for proper get_label() function
        if show_identifier:
            def get_label(node):
                return f'{node.get_tag()}  [{self.extract_identifier(node)}]'
        else:
            def get_label(node):
                return node.get_tag()

        # legacy ordering
        if key is None:
            def key(node):
                return node

        # iter with func
        for pre, node in self.__get(uid, level, filter_func, key, reverse, line_type):
            label = get_label(node)
            func(f'{pre}{label}'.encode('utf-8'))

    def __get(self, uid, level, filter_, key, reverse, line_type):
        # default filter
        if filter_ is None:
            def filter_(node):
                return True

        # render characters
        dt = {
            'ascii': ('|', '|-- ', '+-- '),
            'ascii-ex': ('\u2502', '\u251c\u2500\u2500 ', '\u2514\u2500\u2500 '),
            'ascii-exr': ('\u2502', '\u251c\u2500\u2500 ', '\u2570\u2500\u2500 '),
            'ascii-em': ('\u2551', '\u2560\u2550\u2550 ', '\u255a\u2550\u2550 '),
            'ascii-emv': ('\u2551', '\u255f\u2500\u2500 ', '\u2559\u2500\u2500 '),
            'ascii-emh': ('\u2502', '\u255e\u2550\u2550 ', '\u2558\u2550\u2550 '),
        }[line_type]

        return self.__get_iter(uid, level, filter_, key, reverse, dt, [])

    def __get_iter(self, uid, level, filter_, key, reverse, dt, is_last):
        dt_vline, dt_line_box, dt_line_cor = dt

        uid = self.extract_identifier(self.get_root_node()) if (uid is None) else uid
        if not self.contains(uid):
            raise NodeNotPresentError("Node '%s' is not in the tree" % uid)

        node = self.get_node_for_uid(uid)

        if level == 0:
            yield "", node
        else:
            leading = ''.join(map(lambda x: dt_vline + ' ' * 3
            if not x else ' ' * 4, is_last[0:-1]))
            lasting = dt_line_cor if is_last[-1] else dt_line_box
            yield leading + lasting, node

        if filter_(node):
            children = [self.get_node_for_uid(self.extract_identifier(i)) for i in self.get_child_list_for_uid(self.extract_identifier(node))
                        if filter_(self.get_node_for_uid(self.extract_identifier(i)))]
            idxlast = len(children) - 1
            if key:
                children.sort(key=key, reverse=reverse)
            elif reverse:
                children = reversed(children)
            level += 1
            for idx, child in enumerate(children):
                is_last.append(idx == idxlast)
                for item in self.__get_iter(self.extract_identifier(child), level, filter_,
                                            key, reverse, dt, is_last):
                    yield item
                is_last.pop()
