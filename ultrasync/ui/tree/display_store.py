import logging
import os
from typing import Dict, Union

import gi
from gi.repository.Gtk import TreeIter, TreePath

from fmeta.fmeta_tree_scanner import TreeMetaScanner
from index.uid_generator import UID
from model.display_node import DisplayNode
from model.fmeta_tree import FMetaTree
from model.goog_node import GoogFile
from model.planning_node import PlanningNode

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

from model.fmeta import FMeta

logger = logging.getLogger(__name__)


#    CLASS DisplayStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class DisplayStore:
    """(Mostly) encapsulates the nodes inside the TreeView object, which will be a subset of the nodes
    which come from the data store """
    def __init__(self, treeview_meta):
        self.treeview_meta = treeview_meta
        self.model = Gtk.TreeStore()
        self.model.set_column_types(self.treeview_meta.col_types)

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
        self.checked_rows: Dict[UID, DisplayNode] = {}
        self.inconsistent_rows: Dict[UID, DisplayNode] = {}
        self.displayed_rows: Dict[UID, DisplayNode] = {}

    def get_node_data(self, tree_path: Union[TreeIter, TreePath]) -> DisplayNode:
        """
        Args
            tree_path: TreePath, or TreeIter of target node
        Returns:
            The data node (the contents of the hidden "data" column for the given row
        """
        return self.model[tree_path][self.treeview_meta.col_num_data]

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

    def clear_model(self) -> TreeIter:
        self.model.clear()
        self.checked_rows.clear()
        self.inconsistent_rows.clear()
        self.displayed_rows.clear()
        return self.model.get_iter_first()

    def set_checked_state(self, tree_iter, is_checked, is_inconsistent):
        assert not (is_checked and is_inconsistent)
        node_data: DisplayNode = self.get_node_data(tree_iter)

        if not node_data.has_path():
            # Cannot be checked if no path (LoadingNode, etc.)
            return

        row = self.model[tree_iter]
        row[self.treeview_meta.col_num_checked] = is_checked
        row[self.treeview_meta.col_num_inconsistent] = is_inconsistent

        self.update_checked_state_tracking(node_data, is_checked, is_inconsistent)

    def update_checked_state_tracking(self, node_data: DisplayNode, is_checked: bool, is_inconsistent: bool):
        row_id = node_data.uid
        if is_checked:
            self.checked_rows[row_id] = node_data
        else:
            if row_id in self.checked_rows: del self.checked_rows[row_id]

        if is_inconsistent:
            self.inconsistent_rows[row_id] = node_data
        else:
            if row_id in self.inconsistent_rows: del self.inconsistent_rows[row_id]

    def on_cell_checkbox_toggled(self, widget, path):
        """Called when checkbox in treeview is toggled"""
        data_node: DisplayNode = self.get_node_data(path)
        if not data_node.has_path():
            logger.debug('Disallowing checkbox toggle because node is ephemereal')
            return
        elif self.treeview_meta.is_ignored_func and self.treeview_meta.is_ignored_func(data_node):
            logger.debug('Disallowing checkbox toggle because node is in IGNORED category')
            return
        checked_value = not self.is_node_checked(path)
        logger.debug(f'Toggled {checked_value}: {self.get_node_name(path)}')

        # Need to update all the siblings (children of parent) because their checked state may not be tracked.
        # We can assume that if a parent is not inconsistent (i.e. is either checked or unchecked), the state of its children are implied.
        # But if the parent is inconsistent, we must track the state of ALL of its children.
        tree_iter = self.model.get_iter(path)
        parent_iter = self.model.iter_parent(tree_iter)
        if parent_iter:
            child_iter = self.model.iter_children(parent_iter)
            while child_iter:
                child_data = self.get_node_data(child_iter)
                child_checked = self.model[child_iter][self.treeview_meta.col_num_checked]
                child_inconsistent = self.model[child_iter][self.treeview_meta.col_num_inconsistent]

                self.update_checked_state_tracking(child_data, child_checked, child_inconsistent)

                child_iter = self.model.iter_next(child_iter)

        # Update all of the node's children to match its check state:
        def update_checked_state(t_iter):
            self.set_checked_state(t_iter, checked_value, False)

        self.do_for_self_and_descendants(path, update_checked_state)

        # Now update its ancestors' states:
        tree_path = Gtk.TreePath.new_from_string(path)
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
                self.set_checked_state(tree_iter, is_checked, is_inconsistent)

    # --- Tree searching & iteration (utility functions) --- #

    def _found_func(self, tree_iter, target_uid: UID):
        node = self.get_node_data(tree_iter)
        return not node.is_ephemereal() and node.uid == target_uid

    def find_in_tree(self, target_uid: UID, tree_iter=None):
        """Recurses over entire tree and visits every node until is_found_func() returns True, then returns the data at that node"""
        if not tree_iter:
            tree_iter = self.model.get_iter_first()

        while tree_iter is not None:
            if self._found_func(tree_iter, target_uid):
                return tree_iter
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.find_in_tree(target_uid, child_iter)
            tree_iter = self.model.iter_next(tree_iter)
        return None

    def recurse_over_tree(self, tree_iter, action_func):
        """
        Performs the action_func on the node at this tree_iter AND all of its following
        siblings, and all of their descendants
        """
        while tree_iter is not None:
            action_func(tree_iter)
            if self.model.iter_has_child(tree_iter):
                child_iter = self.model.iter_children(tree_iter)
                self.recurse_over_tree(child_iter, action_func)
            tree_iter = self.model.iter_next(tree_iter)

    def do_for_descendants(self, tree_path, action_func):
        tree_iter = self.model.get_iter(tree_path)
        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_self_and_descendants(self, tree_path, action_func):
        tree_iter = self.model.get_iter(tree_path)
        action_func(tree_iter)

        child_iter = self.model.iter_children(tree_iter)
        if child_iter:
            self.recurse_over_tree(child_iter, action_func)

    def do_for_subtree_and_following_sibling_subtrees(self, tree_path, action_func):
        """
        Includes self and all descendents, and then does the same for all following siblings and their descendants.
        """
        tree_iter = self.model.get_iter(tree_path)
        self.recurse_over_tree(tree_iter, action_func)

    def append_node(self, parent_node_iter, row_values: list):
        data: DisplayNode = row_values[self.treeview_meta.col_num_data]

        if not data.is_ephemereal():
            self.displayed_rows[data.uid] = data

        return self.model.append(parent_node_iter, row_values)

    def remove_first_child(self, parent_iter):
        first_child_iter = self.model.iter_children(parent_iter)
        if not first_child_iter:
            return False

        child_data = self.get_node_data(first_child_iter)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Removing child: {child_data}')

        if not child_data.is_ephemereal():
            self.displayed_rows.pop(child_data.uid)

        # remove the first child
        self.model.remove(first_child_iter)
        return True

    def remove_all_children(self, parent_iter):
        removed_count = 0
        while self.remove_first_child(parent_iter):
            removed_count += 1
        logger.debug(f'Removed {removed_count} children')

    def get_subtree_as_tree(self, tree_path, include_following_siblings=False, checked_only=False):
        """
        FIXME: DEAD CODE. Needs complete rewrite
        Constructs a new FMetaTree out of the data nodes of the subtree referenced
        by tree_path. NOTE: currently the FMeta objects are reused in the new tree,
        for efficiency.
        Args:
            tree_path: root of the subtree, as a GTK3 TreePath
            include_following_siblings: if False, include only the root node and its children
            (filtered by checked state if checked_only is True)
            checked_only: if True, include only rows which are checked
                          if False, include all rows in the subtree
        Returns:
            A new FMetaTree which consists of a subset of the current UI tree
        """
        subtree_root = self.get_node_data(tree_path).full_path
        subtree = FMetaTree(subtree_root)

        def action_func(t_iter):
            if not action_func.checked_only or self.is_node_checked(t_iter):
                data_node: DisplayNode = self.get_node_data(t_iter)
                if isinstance(data_node, FMeta) or isinstance(data_node, GoogFile) or isinstance(data_node, PlanningNode):
                    subtree.add_item(data_node)

        # FIXME this is broken for GDrive
        action_func.checked_only = checked_only

        if include_following_siblings:
            self.do_for_subtree_and_following_sibling_subtrees(tree_path, action_func)
        else:
            self.do_for_self_and_descendants(tree_path, action_func)

        return subtree

    def resync_subtree_dead_code_dont_use(self, tree_path):
        # Construct a FMetaTree from the UI nodes: this is the 'stale' subtree.
        stale_tree = self.get_subtree_as_tree(tree_path)
        fresh_tree = None
        # Master tree contains all FMeta in this widget
        master_tree = self.con.get_tree()

        # If the path no longer exists at all, then it's simple: the entire stale_tree should be deleted.
        if os.path.exists(stale_tree.root_path):
            # But if there are still files present: use FMetaTreeLoader to re-scan subtree
            # and construct a FMetaTree from the 'fresh' data
            logger.debug(f'Scanning: {stale_tree.root_path}')
            scanner = TreeMetaScanner(root_node_identifer=stale_tree.node_identifier, stale_tree=stale_tree, tree_id=self.con.treeview_meta.tree_id, track_changes=False)
            fresh_tree = scanner.scan()

        # TODO: files in different categories are showing up as 'added' in the scan
        # TODO: should just be removed then added below, but brainstorm how to optimize this

        for fmeta in stale_tree.get_all():
            # Anything left in the stale tree no longer exists. Delete it from master tree
            # NOTE: stale tree will contain old FMeta which is from the master tree, and
            # thus does need to have its file path adjusted.
            # This seems awfully fragile...
            old = master_tree.remove(file_path=fmeta.full_path, md5=fmeta.md5, ok_if_missing=False)
            if old:
                logger.debug(f'Deleted from master tree: md5={old.md5} path={old.full_path}')
            else:
                logger.warning(f'Could not delete "stale" from master (not found): md5={fmeta.md5} path={fmeta.full_path}')

        if fresh_tree:
            for fmeta in fresh_tree.get_all():
                # Anything in the fresh tree needs to be either added or updated in the master tree.
                # For the 'updated' case, remove the old FMeta from the file mapping and any old signatures.
                old = master_tree.remove(file_path=fmeta.full_path, md5=fmeta.md5, remove_old_md5=True, ok_if_missing=True)
                if old:
                    logger.debug(f'Removed from master tree: md5={old.md5} path={old.full_path}')
                else:
                    logger.debug(f'Could not delete "fresh" from master (not found): md5={fmeta.md5} path={fmeta.full_path}')
                master_tree.add(fmeta)
                logger.debug(f'Added to master tree: md5={fmeta.md5} path={fmeta.full_path}')

        # 3. Then re-diff and re-populate

        # TODO: Need to introduce a signalling mechanism for the other tree
        logger.info('TODO: re-diff and re-populate!')
