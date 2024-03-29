import logging
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from constants import TreeID
from logging_constants import SUPER_DEBUG_ENABLED
from model.node.node import TNode, SPIDNodePair
from model.node_identifier import GUID
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
        # - When a user checks a row, it goes in 'checked_guid_set' below.
        # - When it is placed in checked_guid_set, it is implied that all its descendants are also checked.
        # - Similarly, when an item is unchecked by the user, all of its descendants are implied to be unchecked.
        # - HOWEVER, un-checking an item will not delete any descendants that may be in the 'checked_guid_set' list.
        #   Anything in the 'checked_guid_set' and 'inconsistent_guid_set' sets is only relevant if its parent is 'inconsistent',
        #   thus, having a parent which is either checked or unchecked overrides any presence in either of these two lists.
        # - At the same time as an item is checked, the checked & inconsistent state of its all ancestors must be recorded.
        # - The 'inconsistent_guid_set' set is needed for display purposes.
        self.checked_guid_set: Set[GUID] = set()
        self.inconsistent_guid_set: Set[GUID] = set()
        self.displayed_guid_dict: Dict[GUID, SPIDNodePair] = {}
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
        self.checked_guid_set.clear()
        self.inconsistent_guid_set.clear()
        self.displayed_guid_dict.clear()
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
            self.checked_guid_set.add(row_id)
        else:
            self.checked_guid_set.discard(row_id)

        if is_inconsistent:
            self.inconsistent_guid_set.add(row_id)
        else:
            self.inconsistent_guid_set.discard(row_id)

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

        # 1. Self and Children
        # Update all the node's children to match its check state:
        def update_checked_state(t_iter):
            self._set_checked_state(t_iter, checked_value, False)

        self.do_for_self_and_descendants(tree_path, update_checked_state)

        # 2. Siblings
        # Need to update all the siblings (children of parent) because their checked state may not be tracked.
        # We can assume that if a parent is not inconsistent (i.e. is either checked or unchecked), the state of its children are implied.
        # But if the parent is inconsistent, we must track the state of ALL of its children.
        tree_iter = self.model.get_iter(tree_path)
        parent_iter = self.model.iter_parent(tree_iter)
        if parent_iter:
            sibling_iter = self.model.iter_children(parent_iter)
            while sibling_iter:
                sibling_data = self.get_node_data(sibling_iter)
                sibling_checked = self.model[sibling_iter][self.treeview_meta.col_num_checked]
                sibling_inconsistent = self.model[sibling_iter][self.treeview_meta.col_num_inconsistent]

                self._update_checked_state_tracking(sibling_data, sibling_checked, sibling_inconsistent)

                sibling_iter = self.model.iter_next(sibling_iter)

        # 3. Ancestors
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
            self.displayed_guid_dict[sn.spid.guid] = sn

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

            self.checked_guid_set.discard(guid)
            self.inconsistent_guid_set.discard(guid)

            if guid in self.displayed_guid_dict:
                del self.displayed_guid_dict[guid]

        self.do_for_self_and_descendants(initial_tree_iter, remove_node_from_lists)

        self.model.remove(initial_tree_iter)

    def remove_loading_node(self, parent_iter):
        """The Loading TNode must be the first child"""
        first_child_iter = self.model.iter_children(parent_iter)
        if not first_child_iter:
            return False

        child_sn = self.get_node_data(first_child_iter)
        if SUPER_DEBUG_ENABLED:
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
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'[{self.tree_id}] Removing 1st child: {child_sn.spid}')

        if not child_sn.node.is_ephemereal():
            self.displayed_guid_dict.pop(child_sn.spid.guid)

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
        return self._execute_on_current_single_selection(self.get_node_data)

    def get_multiple_selection(self) -> List[SPIDNodePair]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.con.tree_view.get_selection()
        model, tree_path_list = selection.get_selected_rows()
        sn_list = []
        for tree_path in tree_path_list:
            sn = self.get_node_data(tree_path)
            sn_list.append(sn)
        return sn_list

    def get_multiple_selection_and_paths(self) -> Tuple[List[SPIDNodePair], List[Gtk.TreePath]]:
        """Returns a list of the selected items (empty if none)"""
        selection = self.con.tree_view.get_selection()
        model, tree_path_list = selection.get_selected_rows()
        sn_list = []
        for tree_path in tree_path_list:
            sn = self.get_node_data(tree_path)
            sn_list.append(sn)
        return sn_list, tree_path_list
