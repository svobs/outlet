import logging
from collections import deque
from typing import Callable, Deque, Dict, Generic, List, Optional, Tuple

from error import NodeAlreadyPresentError, NodeNotPresentError
from logging_constants import TRACE_ENABLED
from util.base_tree import BaseTree, IdentifierT, NodeT

logger = logging.getLogger(__name__)


class SimpleTree(Generic[IdentifierT, NodeT], BaseTree[IdentifierT, NodeT]):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS SimpleTree

    Originally based on a simplifications of treelib.Tree.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, extract_identifier_func: Callable[[NodeT], IdentifierT] = None, extract_node_func: Callable = None):
        BaseTree.__init__(self, extract_identifier_func, extract_node_func)
        self._node_dict: Dict[IdentifierT, NodeT] = {}
        self._parent_child_list_dict: Dict[IdentifierT, List[NodeT]] = {}
        self._child_parent_dict: Dict[IdentifierT, NodeT] = {}
        """Really need this to efficiently execute remove_node()"""
        self._root_node: Optional[NodeT] = None

    def __len__(self):
        return len(self._node_dict)

    def get_node_for_identifier(self, identifier: IdentifierT) -> Optional[NodeT]:
        return self._node_dict.get(identifier, None)

    def get_root_node(self) -> Optional[NodeT]:
        return self._root_node

    def add_node(self, node: NodeT, parent: Optional[NodeT]):
        node_identifier = self.extract_id(node)

        if node_identifier in self._node_dict:
            raise NodeAlreadyPresentError(f'Cannot add node: it is already present in this tree: {node}')

        if not parent:
            if self._root_node:
                raise NodeAlreadyPresentError(f'Cannot add node as root: this tree already has a root node!')
            else:
                self._root_node = node
        elif self.extract_id(parent) not in self._node_dict:
            raise NodeNotPresentError(f'Cannot add node ({node_identifier}): parent "{self.extract_id(parent)}" not found in tree!')
        else:
            parent_identifier = self.extract_id(parent)
            child_list: List[NodeT] = self._parent_child_list_dict.get(parent_identifier, [])
            if not child_list:
                self._parent_child_list_dict[parent_identifier] = child_list
            child_list.append(node)
            self._child_parent_dict[node_identifier] = parent

        self._node_dict[node_identifier] = node

    def swap_with_existing_node(self, node: NodeT) -> NodeT:
        """Swaps just the given node with an existing node in the tree with the same identifier. The previous node's children become the new node's
        children, and the prev's parent becomes its parent. Raises exception if something unexpected happens."""
        node_identifier = self.extract_id(node)
        existing_node = self._node_dict.get(node_identifier, None)
        if not existing_node:
            raise NodeAlreadyPresentError(f'Cannot replace add node: it is not present in this tree: {node}')

        self._node_dict[node_identifier] = node

        parent = self._child_parent_dict.get(node_identifier, None)
        if not parent:
            if self.extract_id(self._root_node) == node_identifier:
                # node is root
                self._root_node = node
                return existing_node
            else:
                raise RuntimeError(f'Bad state: node is not root but has no parent: {existing_node}')
        else:
            parent_identifier = self.extract_id(parent)
            sibling_list: List[NodeT] = self._parent_child_list_dict.get(parent_identifier, [])
            if not sibling_list:
                raise RuntimeError(f'Bad state: no children for parent node: {parent_identifier}')
            for index, sibling in enumerate(sibling_list):
                if sibling == existing_node:
                    sibling_list[index] = node
                    return existing_node
            # should not get here
            raise RuntimeError(f'Bad state: no children for parent node: {parent_identifier}')

    def _remove_node_with_identifier(self, node_list: List[NodeT], identifier: IdentifierT):
        for node in node_list:
            if self.extract_id(node) == identifier:
                node_list.remove(node)
                return node
        return None

    def remove_node(self, identifier: IdentifierT) -> int:
        if self._root_node and self.extract_id(self._root_node) == identifier:
            self._root_node = None
            count_removed: int = len(self._node_dict)
            self._node_dict.clear()
            self._parent_child_list_dict.clear()
            self._child_parent_dict.clear()
            return count_removed

        node = self._node_dict.get(identifier, None)
        if not node:
            raise NodeNotPresentError(f'Cannot remove node: identifier "{identifier}" not found in tree!')

        nid_queue: Deque[IdentifierT] = deque()
        nid_queue.append(identifier)

        # Remove target node from parent's child list
        parent = self.get_parent(identifier)
        if parent:
            child_list: List[NodeT] = self._parent_child_list_dict.get(self.extract_id(parent), None)
            if child_list:
                self._remove_node_with_identifier(child_list, identifier)

        # Now loop and remove target node and all its descendants:
        count_removed = 0
        while len(nid_queue) > 0:
            count_removed += 1
            identifier = nid_queue.popleft()
            removed_node = self._node_dict.pop(identifier, None)
            if not removed_node:
                raise NodeNotPresentError(f'Cannot remove node: it is not present in tree: {identifier}')

            child_list: List[NodeT] = self._parent_child_list_dict.pop(identifier, None)
            if child_list:
                for child in child_list:
                    nid_queue.append(child.identifier)

        return count_removed

    def for_each_node(self, action_func: Callable[[NodeT], None]):
        """Similar to for_each_node_breadth_first(), but should be much faster, with the caveat that order is undefined"""

        if TRACE_ENABLED:
            logger.debug(f'for_each_node(): entering')

        for node in self._node_dict.values():
            action_func(node)

        if TRACE_ENABLED:
            logger.debug(f'for_each_node(): exiting')

    def paste(self, parent_uid: IdentifierT, new_tree):
        root_to_insert = new_tree.get_root_node()
        if not root_to_insert:
            return

        if not parent_uid:
            raise RuntimeError('identifier not provided')

        parent_node = self.get_node_for_identifier(parent_uid)
        if not parent_node:
            raise NodeNotPresentError(f'Node with identifier "{parent_uid}" is not in this tree')

        node_queue: Deque[Tuple[NodeT, NodeT]] = deque()
        node_queue.append((root_to_insert, parent_node))

        count_added = 0
        while len(node_queue) > 0:
            node, parent = node_queue.popleft()
            self.add_node(node, parent)
            count_added += 1
            for child in new_tree.get_child_list_for_identifier(self.extract_id(node)):
                node_queue.append((child, node))
        return count_added

    def get_child_list_for_node(self, node) -> List[NodeT]:
        return self.get_child_list_for_identifier(self.extract_id(node))

    def get_child_list_for_identifier(self, parent_identifier: IdentifierT) -> List[NodeT]:
        if parent_identifier not in self._node_dict:
            raise NodeNotPresentError(f'Cannot get children: parent "{parent_identifier}" is not in the tree!')
        return self._parent_child_list_dict.get(parent_identifier, [])

    def get_parent(self, child_identfier: IdentifierT) -> Optional[NodeT]:
        return self._child_parent_dict.get(child_identfier, None)

    def contains(self, identifier: IdentifierT) -> bool:
        return identifier in self._node_dict

    # The following garbage was copied from treelib.Tree. Should clean this up if there's time
    def show(self, identifier=None, level=0, filter_func=None,
             key=None, reverse=False, line_type='ascii-ex', show_identifier=False):
        """
        Print the tree structure in hierarchy style.

        You have three ways to output your tree data, i.e., stdout with ``show()``,
        plain text file with ``save2file()``, and json string with ``to_json()``. The
        former two use the same backend to generate a string of tree structure in a
        text graph.

        * Version >= 1.2.7a*: you can also specify the ``line_type`` parameter, such as 'ascii' (default), 'ascii-ex', 'ascii-exr', 'ascii-em', 'ascii-emv', 'ascii-emh') to the change graphical form.

        :param identifier: the reference node to start expanding.
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
            self.__print_backend(identifier, level, filter_func,
                                 key, reverse, line_type, show_identifier, func=write)
        except NodeNotPresentError:
            logger.info('Tree is empty')

        return self._reader

    def __print_backend(self, identifier=None, level=0, filter_func=None,
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
                try:
                    the_node = self.extract_node_func(node)
                    tag = the_node.get_tag()
                except Exception:
                    logger.exception(f'Failed to get tag for {node}')
                    tag = '[ERROR]'

                the_id = self.extract_id(node)

                return f'{tag}  [{the_id}]'
        else:
            def get_label(node):
                return self.extract_node_func(node).get_tag()

        # legacy ordering
        if key is None:
            def key(node):
                return node

        # iter with func
        for pre, node in self.__get(identifier, level, filter_func, key, reverse, line_type):
            label = get_label(node)
            func(f'{pre}{label}'.encode('utf-8'))

    def __get(self, identifier, level, filter_, key, reverse, line_type):
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

        return self.__get_iter(identifier, level, filter_, key, reverse, dt, [])

    def __get_iter(self, identifier, level, filter_, key, reverse, dt, is_last):
        dt_vline, dt_line_box, dt_line_cor = dt

        identifier = self.extract_id(self.get_root_node()) if (identifier is None) else identifier
        if not self.contains(identifier):
            raise NodeNotPresentError("Node '%s' is not in the tree" % identifier)

        node = self.get_node_for_identifier(identifier)

        if level == 0:
            yield "", node
        else:
            leading = ''.join(map(lambda x: dt_vline + ' ' * 3
            if not x else ' ' * 4, is_last[0:-1]))
            lasting = dt_line_cor if is_last[-1] else dt_line_box
            yield leading + lasting, node

        if filter_(node):
            children = [self.get_node_for_identifier(self.extract_id(i)) for i in self.get_child_list_for_identifier(self.extract_id(node))
                        if filter_(self.get_node_for_identifier(self.extract_id(i)))]
            idxlast = len(children) - 1
            if key:
                children.sort(key=key, reverse=reverse)
            elif reverse:
                children = reversed(children)
            level += 1
            for idx, child in enumerate(children):
                is_last.append(idx == idxlast)
                for item in self.__get_iter(self.extract_id(child), level, filter_,
                                            key, reverse, dt, is_last):
                    yield item
                is_last.pop()
