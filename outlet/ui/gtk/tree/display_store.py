import logging
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from constants import GDRIVE_PATH_PREFIX, SUPER_DEBUG, TreeID, TreeType
from model.node.node import Node, SPIDNodePair
from model.node_identifier import GUID, SinglePathNodeIdentifier
from model.uid import UID

import gi
gi.require_version("Gtk", "3.0")
from gi.repository.Gtk import TreeIter, TreePath
from gi.repository import Gtk, GLib

logger = logging.getLogger(__name__)


class DisplayStore:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DisplayStore

    (Mostly) encapsulates the nodes inside the TreeView object, which will be a subset of the nodes
    which come from the data store
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, controller, treeview_meta):
        self.con = controller
        self.treeview_meta = treeview_meta
        self.tree_id: TreeID = self.con.tree_id
        self.model: Gtk.TreeStore = Gtk.TreeStore()
        self.model.set_column_types(self.treeview_meta.col_types)
        # Sort by name column at program launch:
        self.model.set_sort_column_id(self.treeview_meta.col_num_name, Gtk.SortType.ASCENDING)

        # Track the checkbox states here. For increased speed and to accommodate lazy loading strategies,
        # we employ the following heuristic:
        # - When a user checks a row, it goes in the 'checked_rows' set below.
        # - When it is placed in the checked_rows set, it is implied that all its descendants are also checked.
        # - Similarly, when an item is unchecked by the user, all of its descendants are implied to be unchecked.
        # - HOWEVER, un-checking an item will not delete any descendants that may be in the 'checked_rows' list.
        #   Anything in the 'checked_rows' and 'inconsistent_rows' lists is only relevant if its parent is 'inconsistent',
        #   thus, having a parent which is either checked or unchecked overrides any presence in either of these two lists.
        # - At the same time as an item is checked, the checked & inconsistent state of its all ancestors must be recorded.
        # - The 'inconsistent_rows' list is needed for display purposes.
        self.checked_rows: Dict[UID, Node] = {}
        self.inconsistent_rows: Dict[UID, Node] = {}
        self.displayed_rows: Dict[GUID, SPIDNodePair] = {}
        """Need to track these so that we can remove a node if its src node was removed"""

    def get_node_data(self, tree_path: Union[TreeIter, TreePath]) -> SPIDNodePair:
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The data node (the contents of the hidden "data" column for the given row
        """
        try:
            return self.model[tree_path][self.treeview_meta.col_num_data]
        except IndexError:
            logger.exception(f'Failed to get node data for tree path ({tree_path})')
            raise RuntimeError(f'An unexpected error occurred while getting node data!')

    def get_node_name(self, tree_path) -> str:
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The value of the 'name' column for the given row
        """
        return self.model[tree_path][self.treeview_meta.col_num_name]

    def is_node_checked(self, tree_path) -> bool:
        return self.model[tree_path][self.treeview_meta.col_num_checked]

    def is_inconsistent(self, tree_path) -> bool:
        return self.model[tree_path][self.treeview_meta.col_num_inconsistent]

    def clear_model_on_ui_thread(self):
        GLib.idle_add(self.clear_model)

    def clear_model(self) -> TreeIter:
        self.model.clear()
        self.checked_rows.clear()
        self.inconsistent_rows.clear()
        self.displayed_rows.clear()
        return self.model.get_iter_first()

    def _set_checked_state(self, tree_iter, is_checked, is_inconsistent):
        assert not (is_checked and is_inconsistent)
        sn: SPIDNodePair = self.get_node_data(tree_iter)

        if sn.node.is_ephemereal():
            # Cannot be checked if no path (LoadingNode, etc.)
            return

        row = self.model[tree_iter]
        row[self.treeview_meta.col_num_checked] = is_checked
        row[self.treeview_meta.col_num_inconsistent] = is_inconsistent

        self._update_checked_state_tracking(sn, is_checked, is_inconsistent)

    def _update_checked_state_tracking(self, sn: SPIDNodePair, is_checked: bool, is_inconsistent: bool):
        row_id = sn.node.uid
        if is_checked:
            self.checked_rows[row_id] = sn.node
        else:
            if row_id in self.checked_rows:
                del self.checked_rows[row_id]

        if is_inconsistent:
            self.inconsistent_rows[row_id] = sn.node
        else:
            if row_id in self.inconsistent_rows:
                del self.inconsistent_rows[row_id]

    def on_cell_checkbox_toggled(self, widget, tree_path):
        """LISTENER/CALLBACK: Called when checkbox in treeview is toggled"""
        if isinstance(tree_path, str):
            tree_path = Gtk.TreePath.new_from_string(tree_path)
        sn: SPIDNodePair = self.get_node_data(tree_path)
        if sn.node.is_ephemereal():
            logger.debug(f'[{self.tree_id}] Disallowing checkbox toggle because node is ephemereal')
            return
        checked_value = not self.is_node_checked(tree_path)
        self.set_row_checked(tree_path, checked_value)

    def set_row_checked(self, tree_path: Gtk.TreePath, checked_value: bool):
        logger.debug(f'[{self.tree_id}] Toggling {checked_value}: {self.get_node_name(tree_path)}')

        # Need to update all the siblings (children of parent) because their checked state may not be tracked.
        # We can assume that if a parent is not inconsistent (i.e. is either checked or unchecked), the state of its children are implied.
        # But if the parent is inconsistent, we must track the state of ALL of its children.
        tree_iter = self.model.get_iter(tree_path)
        parent_iter = self.model.iter_parent(tree_iter)
        if parent_iter:
            child_iter = self.model.iter_children(parent_iter)
            while child_iter:
                child_data = self.get_node_data(child_iter)
                child_checked = self.model[child_iter][self.treeview_meta.col_num_checked]
                child_inconsistent = self.model[child_iter][self.treeview_meta.col_num_inconsistent]

                self._update_checked_state_tracking(child_data, child_checked, child_inconsistent)

                child_iter = self.model.iter_next(child_iter)

        # Update all of the node's children to match its check state:
        def update_checked_state(t_iter):
            self._set_checked_state(t_iter, checked_value, False)

        self.do_for_self_and_descendants(tree_path, update_checked_state)

        # Now update its ancestors' states:
        while True:
            # Go up the tree, one level per loop,
            # with each node updating itself based on its immediate children
            tree_path.up()
            if tree_path.get_depth() < 1:
                # Stop at root
                break
            else:
                tree_iter = self.model.get_iter(tree_path)
                has_checked = False
                has_unchecked = False
                has_inconsistent = False
                child_iter = self.model.iter_children(tree_iter)
                while child_iter is not None:
                    # Parent is inconsistent if any of its children do not match it...
                    if self.is_node_checked(child_iter):
                        has_checked = True
                    else:
                        has_unchecked = True
                    # ...or if any of its children are inconsistent
                    has_inconsistent |= self.is_inconsistent(child_iter)
                    child_iter = self.model.iter_next(child_iter)
                is_checked = has_checked and not has_unchecked and not has_inconsistent
                is_inconsistent = has_inconsistent or (has_checked and has_unchecked)
                self._set_checked_state(tree_iter, is_checked, is_inconsistent)

    # Tree searching & iteration (utility functions)
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    @staticmethod
    def _guid_equals_func(target_guid: GUID, sn: SPIDNodePair) -> bool:
        # if logger.isEnabledFor(logging.DEBUG) and not node.is_ephemeral():
        #     logger.debug(f'Examining node guid={sn.spid.guid} (looking for: {target_guid})')
        return not sn.node.is_ephemereal() and sn.spid.guid == target_guid

    def find_in_tree(self, found_func: Callable[[SPIDNodePair], bool], tree_iter: Optional[Gtk.TreeIter] = None) -> Optional[Gtk.TreeIter]:
        """Generic version:
        Recurses over entire tree and visits every node until is_uid_equals_func() returns True, then returns the data at that node.
        REMEMBER: if this is a lazy-loading tree, this only iterates over the VISIBLE nodes!"""
        if not tree_iter:
            tree_iter = self.model.get_iter_first()

        while tree_iter is not None:
            sn = self.get_node_data(tree_iter)
            if found_func(sn):
                return tree_iter
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                ret_iter = self.find_in_tree(found_func, child_iter)
                if ret_iter:
                    return ret_iter
            tree_iter = self.model.iter_next(tree_iter)
        return None

    def find_guid_in_tree(self, target_guid: GUID, tree_iter: Optional[Gtk.TreeIter] = None) -> Optional[Gtk.TreeIter]:
        """Recurses over entire tree and visits every node until _uid_equals_func() returns True, then returns iter data at that node"""
        bound_func: Callable = partial(self._guid_equals_func, target_guid)
        return self.find_in_tree(bound_func, tree_iter)

    def find_in_children(self, parent_iter, equals_func: Callable[[SPIDNodePair], bool]) -> Optional[Gtk.TreeIter]:
        """Generic version:
        Searches the children of the given parent_iter for the given UID, then returns the iter at that node"""
        if parent_iter:
            if self.model.iter_has_child(parent_iter):
                child_iter = self.model.iter_children(parent_iter)
            else:
                child_iter = None
        else:
            # top level
            child_iter = self.model.get_iter_first()

        while child_iter is not None:
            sn = self.get_node_data(child_iter)
            if equals_func(sn):
                return child_iter
            child_iter = self.model.iter_next(child_iter)
        return None

    def find_guid_in_children(self, target_guid: GUID, parent_iter) -> Optional[Gtk.TreeIter]:
        """Searches the children of the given parent_iter for the given UID, then returns the iter at that node"""
        bound_func: Callable = partial(self._guid_equals_func, target_guid)
        return self.find_in_children(parent_iter, bound_func)

    def get_displayed_children_of(self, parent_guid: GUID) -> List[SPIDNodePair]:
        if not parent_guid:
            child_iter = self.model.get_iter_first()
        else:
            parent_iter = self.find_guid_in_tree(parent_guid)
            if not parent_iter:
                return []
            child_iter = self.model.iter_children(parent_iter)

        child_list: List[SPIDNodePair] = []
        while child_iter is not None:
            sn = self.get_node_data(child_iter)
            if not sn.node.is_ephemereal():
                child_list.append(sn)
            child_iter = self.model.iter_next(child_iter)

        return child_list

    def recurse_over_tree(self, tree_iter: Gtk.TreeIter = None, action_func: Callable[[Gtk.TreeIter], None] = None):
        """
        Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants
        """
        if not tree_iter:
            tree_iter = self.model.get_iter_first()

        while tree_iter is not None:
            action_func(tree_iter)
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.recurse_over_tree(child_iter, action_func)
            tree_iter = self.model.iter_next(tree_iter)

    def do_for_descendants(self, tree_path: Union[Gtk.TreeIter, Gtk.TreePath], action_func: Callable[[Gtk.TreeIter], None]):
        if isinstance(tree_path, Gtk.TreeIter):
            tree_iter = tree_path
        else:
            tree_iter = self.model.get_iter(tree_path)
        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_self_and_descendants(self, tree_path: Union[Gtk.TreeIter, Gtk.TreePath], action_func: Callable[[Gtk.TreeIter], None]):
        if isinstance(tree_path, Gtk.TreeIter):
            tree_iter = tree_path
        else:
            tree_iter = self.model.get_iter(tree_path)
        action_func(tree_iter)

        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_subtree_and_following_sibling_subtrees(self, tree_path: Union[Gtk.TreeIter, Gtk.TreePath],
                                                      action_func: Callable[[Gtk.TreeIter], None]):
        """
        Includes self and all descendents, and then does the same for all following siblings and their descendants.
        """
        if isinstance(tree_path, Gtk.TreeIter):
            tree_iter = tree_path
        else:
            tree_iter = self.model.get_iter(tree_path)
        self.recurse_over_tree(tree_iter, action_func)

    def append_node(self, parent_node_iter, row_values: list):
        sn: SPIDNodePair = row_values[self.treeview_meta.col_num_data]

        if not sn.node.is_ephemereal():
            self.displayed_rows[sn.spid.guid] = sn

        return self.model.append(parent_node_iter, row_values)

    def remove_node(self, node_guid: GUID):
        """Also removes itself and any descendents from the lists"""

        # TODO: this can be optimized to search only the paths of the ancestors
        initial_tree_iter = self.find_guid_in_tree(target_guid=node_guid)
        if not initial_tree_iter:
            raise RuntimeError(f'Could not find node in display tree with GUID: {node_guid}')

        def remove_node_from_lists(tree_iter):
            sn = self.get_node_data(tree_iter)
            if sn.node.is_ephemereal():
                return

            guid = sn.spid.guid
            uid = sn.node.uid

            if uid in self.checked_rows:
                del self.checked_rows[uid]
            if uid in self.inconsistent_rows:
                del self.inconsistent_rows[uid]

            if guid in self.displayed_rows:
                del self.displayed_rows[guid]

        self.do_for_self_and_descendants(initial_tree_iter, remove_node_from_lists)

        self.model.remove(initial_tree_iter)

    def remove_loading_node(self, parent_iter):
        """The Loading Node must be the first child"""
        first_child_iter = self.model.iter_children(parent_iter)
        if not first_child_iter:
            return False

        child_sn = self.get_node_data(first_child_iter)
        if SUPER_DEBUG:
            logger.debug(f'[{self.tree_id}] Removing child: {child_sn.spid}')

        if not child_sn.node.is_ephemereal():
            logger.error(f'[{self.tree_id}] Expected LoadingNode but found: {child_sn.node}')
            return

        # remove the first child
        self.model.remove(first_child_iter)
        return True

    def remove_first_child(self, parent_iter):
        first_child_iter = self.model.iter_children(parent_iter)
        if not first_child_iter:
            return False

        child_sn = self.get_node_data(first_child_iter)
        if SUPER_DEBUG:
            logger.debug(f'[{self.tree_id}] Removing 1st child: {child_sn.spid}')

        if not child_sn.node.is_ephemereal():
            self.displayed_rows.pop(child_sn.spid.guid)

        # remove the first child
        self.model.remove(first_child_iter)
        return True

    def ensure_tree_iter(self, tree_thing: Union[Gtk.TreeIter, Gtk.TreePath]) -> TreeIter:
        if isinstance(tree_thing, Gtk.TreeIter):
            return tree_thing
        else:
            return self.model.get_iter(tree_thing)

    def ensure_tree_path(self, tree_thing: Union[Gtk.TreeIter, Gtk.TreePath]) -> TreePath:
        if isinstance(tree_thing, Gtk.TreePath):
            return tree_thing
        else:
            return self.model.get_path(tree_thing)

    def remove_all_children(self, parent_iter):
        removed_count = 0
        while self.remove_first_child(parent_iter):
            removed_count += 1
        logger.debug(f'[{self.tree_id}] Removed {removed_count} children')

    # FIXME: deprecate this
    def build_spid_from_tree_path(self, tree_path: Gtk.TreePath) -> SinglePathNodeIdentifier:
        node = self.get_node_data(tree_path)
        single_path = self.derive_single_path_from_tree_path(tree_path)
        return SinglePathNodeIdentifier(uid=node.uid, path_list=single_path, tree_type=node.tree_type)

    # FIXME: deprecate this
    def build_sn_from_tree_path(self, tree_path: Union[TreeIter, TreePath]) -> SPIDNodePair:
        node = self.get_node_data(tree_path)
        single_path = self.derive_single_path_from_tree_path(tree_path)
        spid = SinglePathNodeIdentifier(uid=node.uid, path_list=single_path, tree_type=node.tree_type)
        return SPIDNodePair(spid, node)

    def derive_single_path_from_tree_path(self, tree_path: Gtk.TreePath, include_gdrive_prefix: bool = False) -> str:
        """Travels up the display tree and constructs a single path for the given node.
        Some background: while a node can have several paths associated with it, each display tree node can only be associated
        with a single path - but that information is implicit in the tree structure itself and must be reconstructed from it."""
        tree_path = self.ensure_tree_path(tree_path)

        # FIXME!
        logger.warning(f'derive_single_path_from_tree_path() should not be called with an active filter! '
                       f'Will arbitrarily choose the first path in list')
        # if self.treeview_meta.filter_criteria and self.treeview_meta.filter_criteria.has_criteria():
        #     # FIXME: this may choose a less correct path when a filter is applied
        #     logger.warning(f'derive_single_path_from_tree_path() should not be called with an active filter!'
        #                    f'Will arbitrarily choose the first path in list')
        #     node = self.get_node_data(tree_path)
        #     return node.get_path_list()[0]
        # don't mess up the caller; make a copy before modifying:
        tree_path_copy = tree_path.copy()

        node = self.get_node_data(tree_path_copy)
        is_gdrive: bool = node.tree_type == TreeType.GDRIVE

        single_path = ''

        while True:
            node = self.get_node_data(tree_path_copy)
            single_path = f'/{node.name}{single_path}'
            # Go up the tree, one level per loop,
            # with each node updating itself based on its immediate children
            tree_path_copy.up()
            if tree_path_copy.get_depth() < 1:
                # Stop at root
                break

        base_path = self.con.get_tree().get_root_spid().get_single_path()
        if base_path != '/':
            single_path = f'{base_path}{single_path}'

        if is_gdrive and include_gdrive_prefix:
            single_path = f'{GDRIVE_PATH_PREFIX}{single_path}'
        logger.debug(f'derive_single_path_from_tree_path(): derived path: {single_path}')
        return single_path

    def _execute_on_current_single_selection(self, action_func: Callable[[Gtk.TreePath], Any]):
        """Assumes that only one node can be selected at a given time"""
        selection = self.con.tree_view.get_selection()
        model, tree_path_list = selection.get_selected_rows()
        if len(tree_path_list) == 1:
            tree_path = tree_path_list[0]
            return action_func(tree_path)
        elif len(tree_path_list) == 0:
            return None
        else:
            raise Exception(f'Selection has more rows than expected: count={len(tree_path_list)}')

    def get_single_selection_sn(self) -> SPIDNodePair:
        return self._execute_on_current_single_selection(self.build_sn_from_tree_path)

    def get_single_selection(self) -> Node:
        return self._execute_on_current_single_selection(lambda tp: self.get_node_data(tp))

    def get_multiple_selection(self) -> List[SPIDNodePair]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.con.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        sn_list = []
        for tree_path in tree_paths:
            sn = self.get_node_data(tree_path)
            sn_list.append(sn)
        return sn_list

    def get_multiple_selection_and_paths(self) -> Tuple[List[Node], List[Gtk.TreePath]]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.con.tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        items = []
        for tree_path in tree_paths:
            item = self.get_node_data(tree_path)
            items.append(item)
        return items, tree_paths
