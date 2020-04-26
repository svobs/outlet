"""
Populates the DiffTreePanel's model from a given FMetaTree.
"""
from datetime import datetime
import os
import humanfriendly
import logging

from stopwatch import Stopwatch
from treelib import Tree
import file_util
from model.fmeta import FMeta, Category
from fmeta.fmeta_tree_loader import TreeMetaScanner
from ui.tree.data_store import DisplayStrategy
from model.display_model import DirNode, CategoryNode
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib

logger = logging.getLogger(__name__)


def _build_category_change_tree(fmeta_list, category, root_path):
    """
    Builds a tree out of the flat change set.
    Args:
        fmeta_list: source tree for category
        cat_name: the category name

    Returns:
        change tree
    """
    # The change set in tree form
    change_tree = Tree()  # from treelib

    set_len = len(fmeta_list)
    if set_len == 0:
        return change_tree

    logger.info(f'Building change trees for category {category.name} with {set_len} files...')

    root = change_tree.create_node(tag=f'{category.name} ({set_len} files)', identifier='', data=CategoryNode(root_path, category))   # root
    for fmeta in fmeta_list:
        dirs_str, file_name = os.path.split(fmeta.get_relative_path(root_path))
        # nid == Node ID == directory name
        nid = ''
        parent = root
        logger.debug(f'Adding root file "{fmeta.full_path}" to dir "{parent.data.full_path}"')
        parent.data.add_meta(fmeta)
        if dirs_str != '':
            path_segments = file_util.split_path(dirs_str)
            for dir_name in path_segments:
                nid = os.path.join(nid, dir_name)
                child = change_tree.get_node(nid=nid)
                if child is None:
                    dir_full_path = os.path.join(root_path, nid)
                    logger.debug(f'Creating dir node: nid={nid}')
                    child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=DirNode(dir_full_path, category))
                parent = child
                logger.debug(f'Adding file node nid="{fmeta.full_path}" to dir node {parent.data.full_path}"')
                parent.data.add_meta(fmeta)
        nid = os.path.join(nid, file_name)
        logger.debug(f'Creating file node: nid={nid}')
        change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

    return change_tree


class FMetaChangeTreeStrategy(DisplayStrategy):
    def __init__(self, controller=None):
        super().__init__(controller)

    def init(self):
        pass

    def _append_dir_node(self, tree_iter, dir_name, node_data):
        """Appends a dir or cat node to the model"""
        row_values = []
        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append('folder')  # Icon
        row_values.append(dir_name)  # Name
        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory
        num_bytes_str = humanfriendly.format_size(node_data.size_bytes)
        row_values.append(num_bytes_str)  # Size
        row_values.append(None)  # Modify Date
        if self.con.display_store.display_meta.show_change_ts:
            row_values.append(None)  # Modify Date
        row_values.append(node_data)  # Data

        return self.con.display_store.model.append(tree_iter, row_values)

    def _append_fmeta_node(self, tree_iter, file_name, fmeta: FMeta, category):
        row_values = []

        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(category.name)  # Icon

        if category == Category.Moved:
            node_name = f'{fmeta.prev_path} -> "{file_name}"'
        else:
            node_name = file_name
        row_values.append(node_name)  # Name

        if not self.con.display_store.display_meta.use_dir_tree:
            directory, name = os.path.split(fmeta.full_path)
            row_values.append(directory)  # Directory

        num_bytes_str = humanfriendly.format_size(fmeta.size_bytes)
        row_values.append(num_bytes_str)  # Size

        modify_datetime = datetime.fromtimestamp(fmeta.modify_ts)
        modify_time = modify_datetime.strftime(self.con.display_store.display_meta.datetime_format)
        row_values.append(modify_time)  # Modify TS

        if self.con.display_store.display_meta.show_change_ts:
            change_datetime = datetime.fromtimestamp(fmeta.change_ts)
            change_time = change_datetime.strftime(self.con.display_store.display_meta.datetime_format)
            row_values.append(change_time)  # Change TS

        row_values.append(fmeta)  # Data
        return self.con.display_store.model.append(tree_iter, row_values)

    def _append_to_model(self, category, change_tree):
        def append_recursively(tree_iter, node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if isinstance(node.data, DirNode):
                # Is dir
                tree_iter = self._append_dir_node(tree_iter, node.tag, node.data)
                for child in change_tree.children(node.identifier):
                    append_recursively(tree_iter, child)
            else:
                self._append_fmeta_node(tree_iter, node.tag, node.data, category)

        if change_tree.size(1) > 0:
            # logger.debug(f'Appending category: {category.name}')
            root = change_tree.get_node('')
            append_recursively(None, root)

    def _set_expand_states_from_config(self):
        # Loop over top level. Find the category nodes and expand them appropriately
        tree_iter = self.con.display_store.model.get_iter_first()
        while tree_iter is not None:
            node_data = self.con.display_store.get_node_data(tree_iter)
            if type(node_data) == CategoryNode:
                is_expand = self.con.display_store.display_meta.is_category_node_expanded(node_data)
                if is_expand:
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    self.con.tree_view.expand_row(path=tree_path, open_all=True)

            tree_iter = self.con.display_store.model.iter_next(tree_iter)

    def populate_root(self):
        """
        Populates the given DiffTreePanel using categories as the topmost elements.
        TODO: looks like we'll have to implement lazy loading to speed things up...
        Args:
            diff_tree: the DiffTreePanel widget

        """
        logger.debug(f'Repopulating diff tree "{self.con.data_store.tree_id}"')

        fmeta_tree = self.con.data_store.get_whole_tree()

        # Wipe out existing items:
        self.con.display_store.model.clear()

        change_trees = {}
        for category in [Category.Added,
                         Category.Deleted,
                         Category.Moved,
                         Category.Updated,
                         Category.Ignored]:
            # Build fake tree for category:
            stopwatch = Stopwatch()
            change_tree = _build_category_change_tree(fmeta_tree.get_for_cat(category), category, fmeta_tree.root_path)
            logger.debug(f'Faux tree built for "{category.name}" in: {stopwatch}')
            change_trees[category] = change_tree

        def update_ui():
            stopwatch = Stopwatch()
            for cat, tree in change_trees.items():
                self._append_to_model(cat, tree)
            logger.debug(f'TreeStore populated in: {stopwatch}')

            # Restore user prefs for expanded nodes:
            self._set_expand_states_from_config()
            logger.debug(f'Done repopulating diff tree "{self.con.data_store.tree_id}"')

        GLib.idle_add(update_ui)

    def resync_subtree(self, tree_path):
        # Construct a FMetaTree from the UI nodes: this is the 'stale' subtree.
        stale_tree = self.con.display_store.get_subtree_as_tree(tree_path)
        fresh_tree = None
        # Master tree contains all FMeta in this widget
        master_tree = self.con.data_store.get_whole_tree()

        # If the path no longer exists at all, then it's simple: the entire stale_tree should be deleted.
        if os.path.exists(stale_tree.root_path):
            # But if there are still files present: use FMetaTreeLoader to re-scan subtree
            # and construct a FMetaTree from the 'fresh' data
            logger.debug(f'Scanning: {stale_tree.root_path}')
            scanner = TreeMetaScanner(root_path=stale_tree.root_path, stale_tree=stale_tree, tree_id=self.con.data_store.tree_id, track_changes=False)
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
