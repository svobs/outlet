"""
Populates the DiffTreePanel's model from a given FMetaTree.
"""
import logging
import os

import gi
import treelib

import file_util
from constants import TreeDisplayMode
from ui.tree.category_tree_builder import CategoryTreeBuilder

gi.require_version("Gtk", "3.0")
from gi.repository import GLib

from fmeta.fmeta_tree_scanner import TreeMetaScanner

from ui import actions
from ui.tree.lazy_display_strategy import LazyDisplayStrategy


logger = logging.getLogger(__name__)


class FMetaChangeTreeStrategy(LazyDisplayStrategy):
    def __init__(self, config, controller=None):
        super().__init__(config, controller)

    def init(self):
        super().init()

    def _append_full_category_tree_to_model(self, category_tree: treelib.Tree):
        def append_recursively(parent_iter, parent_uid, node: treelib.Node):
            # Do a DFS of the change tree and populate the UI tree along the way
            if node.data.is_dir():
                parent_iter = self._append_dir_node(parent_iter=parent_iter, parent_uid=parent_uid, node_data=node.data)

                child_relative_path = file_util.strip_root(node.data.full_path, self.con.tree_builder.get_root_identifier().full_path)

                for child in category_tree.children(child_relative_path):
                    append_recursively(parent_iter, parent_uid, child)
            else:
                self._append_file_node(parent_iter, parent_uid, node.data)

        if category_tree.size(1) > 0:
            # logger.debug(f'Appending category: {category.name}')
            root_node: treelib.Node = category_tree.get_node(category_tree.root)
            append_recursively(None, None, root_node)

    def populate_root(self):
        logger.debug(f'Repopulating tree "{self.con.treeview_meta.tree_id}"')

        if self.con.treeview_meta.lazy_load:
            super().populate_root()
        else:
            # All at once
            # KLUDGE! This is not even supported for GDrive
            assert isinstance(self.con.tree_builder, CategoryTreeBuilder)
            category_trees = self.con.tree_builder.get_category_trees_static()

            def update_ui():
                # Wipe out existing items:
                self.con.display_store.clear_model()

                for category_tree in category_trees:
                    self._append_full_category_tree_to_model(category_tree)

                # Restore user prefs for expanded nodes:
                self._set_expand_states_from_config()
                logger.debug(f'Done repopulating diff tree "{self.con.treeview_meta.tree_id}"')

            GLib.idle_add(update_ui)

            # Show tree summary:
            actions.set_status(sender=self.con.treeview_meta.tree_id,
                               status_msg=self.con.get_tree().get_summary())

    def resync_subtree(self, tree_path):
        # Construct a FMetaTree from the UI nodes: this is the 'stale' subtree.
        stale_tree = self.con.display_store.get_subtree_as_tree(tree_path)
        fresh_tree = None
        # Master tree contains all FMeta in this widget
        master_tree = self.con.get_tree()

        # If the path no longer exists at all, then it's simple: the entire stale_tree should be deleted.
        if os.path.exists(stale_tree.root_path):
            # But if there are still files present: use FMetaTreeLoader to re-scan subtree
            # and construct a FMetaTree from the 'fresh' data
            logger.debug(f'Scanning: {stale_tree.root_path}')
            scanner = TreeMetaScanner(root_path=stale_tree.root_path, stale_tree=stale_tree, tree_id=self.con.treeview_meta.tree_id, track_changes=False)
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
