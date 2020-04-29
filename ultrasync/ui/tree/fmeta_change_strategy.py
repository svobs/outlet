"""
Populates the DiffTreePanel's model from a given FMetaTree.
"""
from datetime import datetime
import os
import humanfriendly
import logging

from pydispatch import dispatcher
from fmeta.fmeta_tree_loader import TreeMetaScanner
from model.display_node import DirNode, CategoryNode, DisplayNode
import gi

from model.planning_node import FileToMove
from ui import actions
from ui.tree import category_tree_builder
from ui.tree.display_strategy import DisplayStrategy

gi.require_version("Gtk", "3.0")
from gi.repository import GLib

logger = logging.getLogger(__name__)


class FMetaChangeTreeStrategy(DisplayStrategy):
    def __init__(self, controller=None):
        super().__init__(controller)

    def init(self):
        super().init()
        dispatcher.connect(signal=actions.NODE_EXPANSION_TOGGLED, receiver=self._on_node_expansion_toggled, sender=self.con.data_store.tree_id)
        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=self.con.data_store.tree_id)

    def _on_root_path_updated(self, sender, new_root):
        # Get a new metastore from the cache manager:
        self.con.data_store = self.con.parent_win.application.cache_manager.get_metastore_for_local_subtree(new_root, self.con.data_store.tree_id)

    def _append_children(self, children, parent_iter):
        if children:
            logger.debug(f'Filling out display children: {len(children)}')
            # Append all underneath tree_iter
            for child in children:
                if child.is_dir():
                    self.append_dir_node_and_empty_child(parent_iter, child)
                else:
                    self._append_file_node(parent_iter, child)
        elif self.use_empty_nodes:
            self._append_empty_child(parent_iter)

    def _on_node_expansion_toggled(self, sender, parent_iter, node_data, is_expanded):
        logger.debug(f'Node expansion toggled to {is_expanded} for cat={node_data.category} path="{node_data.full_path}"')

        if not self.con.data_store.is_lazy():
            return

        # FIXME: checkboxes + lazy load

        # Add children for node:
        if is_expanded:
            children = self.con.data_store.get_children(node_data.display_id)
            self._append_children(children, parent_iter)
            # Remove Loading node:
            self.con.display_store.remove_first_child(parent_iter)
        else:
            # Collapsed:
            self.con.display_store.remove_all_children(parent_iter)
            # Always have at least a dummy node:
            self._append_loading_child(parent_iter)

    def append_dir_node(self, tree_iter, node_data: DisplayNode):
        """Appends a dir or cat node to the model"""
        row_values = []

        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent

        row_values.append('folder')  # Icon

        row_values.append(node_data.get_name())  # Name

        if not self.con.display_store.display_meta.use_dir_tree:
            row_values.append(None)  # Directory

        if not node_data.size_bytes:
            num_bytes_formatted = None
        else:
            num_bytes_formatted = humanfriendly.format_size(node_data.size_bytes)
        row_values.append(num_bytes_formatted)  # Size

        row_values.append(None)  # Modify Date

        if self.con.display_store.display_meta.show_change_ts:
            row_values.append(None)  # Modify Date

        row_values.append(node_data)  # Data

        return self.con.display_store.model.append(tree_iter, row_values)

    def _append_file_node(self, tree_iter, node_data: DisplayNode):
        row_values = []

        if self.con.display_store.display_meta.editable:
            row_values.append(False)  # Checked
            row_values.append(False)  # Inconsistent
        row_values.append(node_data.category.name)  # Icon

        if isinstance(node_data, FileToMove):
            node_name = f'{node_data.original_full_path} -> "{node_data.get_name()}"'
        else:
            node_name = node_data.get_name()
        row_values.append(node_name)  # Name

        if not self.con.display_store.display_meta.use_dir_tree:
            directory, name = os.path.split(node_data.full_path)
            row_values.append(directory)  # Directory

        num_bytes_str = humanfriendly.format_size(node_data.size_bytes)
        row_values.append(num_bytes_str)  # Size

        modify_datetime = datetime.fromtimestamp(node_data.modify_ts)
        modify_time = modify_datetime.strftime(self.con.display_store.display_meta.datetime_format)
        row_values.append(modify_time)  # Modify TS

        if self.con.display_store.display_meta.show_change_ts:
            change_datetime = datetime.fromtimestamp(node_data.change_ts)
            change_time = change_datetime.strftime(self.con.display_store.display_meta.datetime_format)
            row_values.append(change_time)  # Change TS

        row_values.append(node_data)  # Data
        return self.con.display_store.model.append(tree_iter, row_values)

    def _set_expand_states_from_config(self):
        # Loop over top level. Find the category nodes and expand them appropriately
        tree_iter = self.con.display_store.model.get_iter_first()
        while tree_iter is not None:
            node_data = self.con.display_store.get_node_data(tree_iter)
            if type(node_data) == CategoryNode:
                is_expand = self.con.display_store.display_meta.is_category_node_expanded(node_data)
                if is_expand:
                    tree_path = self.con.display_store.model.get_path(tree_iter)
                    logger.info(f'Expanding row: {node_data.get_name()} in tree {self.con.tree_id}')
                    self.con.tree_view.expand_row(path=tree_path, open_all=True)
                    # TODO! Listeners

            tree_iter = self.con.display_store.model.iter_next(tree_iter)

    def _append_to_model(self, change_tree):
        def append_recursively(tree_iter, node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if isinstance(node.data, DirNode):
                # Is dir
                tree_iter = self.append_dir_node(tree_iter, node.data)
                for child in change_tree.children(node.identifier):
                    append_recursively(tree_iter, child)
            else:
                self._append_file_node(tree_iter, node.data)

        if change_tree.size(1) > 0:
            # logger.debug(f'Appending category: {category.name}')
            root = change_tree.get_node(change_tree.root)
            append_recursively(None, root)

    def populate_root(self):
        logger.debug(f'Repopulating diff tree "{self.con.data_store.tree_id}"')

        if self.con.data_store.is_lazy():
            # This may be a long task
            children = self.con.data_store.get_children(parent_id=None)

            def update_ui():
                # Wipe out existing items:
                root_iter = self.con.display_store.clear_model()

                self._append_children(children, root_iter)

                # This should fire expanded state listener to populate nodes as needed:
                self._set_expand_states_from_config()

            GLib.idle_add(update_ui)
        else:
            # KLUDGE
            category_trees = self.con.data_store.get_category_trees()

            def update_ui():
                # Wipe out existing items:
                self.con.display_store.clear_model()

                for tree in category_trees:
                    self._append_to_model(tree)

                # Restore user prefs for expanded nodes:
                self._set_expand_states_from_config()
                logger.debug(f'Done repopulating diff tree "{self.con.data_store.tree_id}"')

            GLib.idle_add(update_ui)

        # Show tree summary:
        actions.set_status(sender=self.con.data_store.tree_id,
                           status_msg=self.con.data_store.get_whole_tree().get_summary())

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
