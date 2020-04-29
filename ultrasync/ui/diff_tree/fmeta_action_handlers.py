import os
import logging
import subprocess
import ui.actions as actions
from model.planning_node import FMetaDecorator
from ui.tree.action_bridge import TreeActionBridge
from model.display_node import DirNode, CategoryNode

import gi
gi.require_version("Gtk", "3.0")
from gi.repository import Gtk

logger = logging.getLogger(__name__)


class FMetaTreeActionHandlers(TreeActionBridge):
    def __init__(self, controller=None):
        super().__init__(controller)

    def init(self):
        super().init()
        actions.connect(actions.NODE_EXPANSION_TOGGLED, self._on_node_expansion_toggled, sender=self.con.tree_id)

    # --- LISTENERS ---

    def on_single_row_activated(self, tree_view, tree_iter, tree_path):
        """Fired when an item is double-clicked or when an item is selected and Enter is pressed"""
        node_data = self.con.display_store.get_node_data(tree_iter)
        if type(node_data) == CategoryNode:
            # Special handling for categories: toggle collapse state
            if tree_view.row_expanded(tree_path):
                tree_view.collapse_row(tree_path)
            else:
                tree_view.expand_row(path=tree_path, open_all=False)
        elif node_data.has_path():
            if os.path.exists(node_data.full_path):
                self.call_xdg_open(node_data.full_path)
            elif isinstance(node_data, FMetaDecorator):
                if os.path.exists(node_data.original_full_path):
                    self.call_xdg_open(node_data.original_full_path)
                else:
                    logger.debug(f'Path does not exist: {node_data.original_full_path}')
            else:
                logger.debug(f'Path does not exist: {node_data.full_path}')
        else:
            logger.debug(f'Unexpected data element: {type(node_data)}')

    def on_multiple_rows_activated(self, tree_view, tree_iter):
        # TODO: intelligent logic for multiple selected rows
        logger.error('Multiple rows activated, but no logic implemented yet!')
        pass

    def _on_node_expansion_toggled(self, sender, parent_iter, node_data, is_expanded, expand_all=False):
        return False

    def build_context_menu(self, tree_path: Gtk.TreePath, node_data):
        """Dynamic context menu (right-click on tree item)"""

        if not node_data.has_path():
            # 'Loading' node, 'Empty' node, etc.
            return

        menu = Gtk.Menu()

        if isinstance(node_data, FMetaDecorator):
            full_path = node_data.original_full_path
        else:
            full_path = node_data.full_path
        parent_path, file_name = os.path.split(full_path)

        is_category_node = type(node_data) == CategoryNode
        file_exists = os.path.exists(full_path)

        item = Gtk.MenuItem(label='')
        label = item.get_child()
        label.set_markup(f'<i>{full_path}</i>')
        item.set_sensitive(False)
        menu.append(item)

        item = Gtk.SeparatorMenuItem()
        menu.append(item)

        if file_exists:
            item = Gtk.MenuItem(label='Show in Nautilus')
            item.connect('activate', lambda menu_item, f: self.show_in_nautilus(f), full_path)
            menu.append(item)
        else:
            item = Gtk.MenuItem(label='')
            label = item.get_child()
            label.set_markup(f'<i>Path not found</i>')
            item.set_sensitive(False)
            menu.append(item)

        if os.path.isdir(full_path):
            item = Gtk.MenuItem(label=f'Expand all')
            item.connect('activate', lambda menu_item: self.expand_all(tree_path))
            menu.append(item)

            if not is_category_node and file_exists:
                item = Gtk.MenuItem(label=f'Delete tree "{file_name}"')
                item.connect('activate', lambda menu_item, abs_p: self.delete_dir_tree(abs_p, tree_path), full_path)
                menu.append(item)
        elif file_exists:
            item = Gtk.MenuItem(label=f'Delete "{file_name}"')
            item.connect('activate', lambda menu_item, abs_p: self.delete_single_file(abs_p, tree_path), full_path)
            menu.append(item)

        menu.show_all()
        return menu

    def on_delete_key_pressed(self, selected_tree_paths):
        # Get the TreeIter instance for each path
        for tree_path in selected_tree_paths:
            # Delete the actual file:
            node_data = self.con.display_store.get_node_data(tree_path)
            if node_data is not None:
                if not self.delete_dir_tree(subtree_root=node_data.full_path, tree_path=tree_path):
                    # something went wrong if we got False. Stop.
                    break

    def on_row_right_clicked(self, event, tree_path, node_data):
        # Display context menu:
        context_menu = self.build_context_menu(tree_path, node_data)
        context_menu.popup_at_pointer(event)

    # --- END of LISTENERS ---

    # --- ACTIONS ---

    def delete_single_file(self, file_path: str, tree_path: Gtk.TreePath):
        """ Param file_path must be an absolute path"""
        if os.path.exists(file_path):
            try:
                logger.info(f'Deleting file: {file_path}')
                os.remove(file_path)
            except Exception as err:
                self.con.parent_win.show_error_msg(f'Error deleting file "{file_path}"', str(err))
                raise
            finally:
                self.con.display_store.resync_subtree(tree_path)
        else:
            self.con.parent_win.show_error_msg('Could not delete file', f'Not found: {file_path}')

    def expand_all(self, tree_path):
        # TODO
        pass

    def delete_dir_tree(self, subtree_root: str, tree_path: Gtk.TreePath):
        """
        Param subtree_root must be an absolute path.
        This will delete the files corresponding to the UI tree -
        which may NOT represent all the files in the corresponding filesystem tree!
        If a directory is found to be empty after we are done deleting files in it,
        we will delete the directory as well.
        """
        if not os.path.exists(subtree_root):
            self.con.parent_win.show_error_msg('Could not delete tree', f'Not found: {subtree_root}')
            return False
        logger.info(f'User chose to delete subtree: {subtree_root}')

        dir_count = 0

        try:
            root_path = self.con.data_store.get_root_path()
            # We will populate this with files and directories we encounter
            # doing a DFS of the subtree root:
            path_list = []

            def add_to_list_func(t_iter):
                data_node = self.con.display_store.get_node_data(t_iter)
                p = data_node.full_path
                path_list.append(p)
                if os.path.isdir(p):
                    add_to_list_func.dir_count += 1

            add_to_list_func.dir_count = 0

            self.con.display_store.do_for_self_and_descendants(tree_path, add_to_list_func)

            dir_count = add_to_list_func.dir_count
        except Exception as err:
            self.con.parent_win.show_error_msg(f'Error collecting file list for "{subtree_root}"', str(err))
            raise

        file_count = len(path_list) - dir_count
        msg = f'Are you sure you want to delete the {file_count} files in {subtree_root}?'
        is_confirmed = self.con.parent_win.show_question_dialog('Confirm subtree deletion',
                                                                secondary_msg=msg)
        if not is_confirmed:
            logger.debug('User cancelled delete')
            return

        try:
            logger.info(f'About to delete {file_count} files and up to {dir_count} dirs')
            # By going backwards, we iterate from the bottom to top of tree.
            # This guarantees that we examine the files before their parent dirs.
            for path_to_delete in path_list[-1::-1]:
                if os.path.isdir(path_to_delete):
                    if not os.listdir(path_to_delete):
                        logger.info(f'Deleting empty dir: {path_to_delete}')
                        os.rmdir(path_to_delete)
                else:
                    logger.info(f'Deleting file: {path_to_delete}')
                    os.remove(path_to_delete)

        except Exception as err:
            self.con.parent_win.show_error_msg(f'Error deleting tree "{subtree_root}"', str(err))
            raise
        finally:
            # TODO: make this into a signal
            self.con.display_store.resync_subtree(tree_path)

    def call_xdg_open(self, file_path):
        if os.path.exists(file_path):
            logger.info(f'Calling xdg-open for: {file_path}')
            subprocess.check_call(["xdg-open", file_path])
        else:
            self.con.parent_win.show_error_msg(f'Cannot open file', f'File not found: {file_path}')

    def show_in_nautilus(self, file_path):
        if os.path.exists(file_path):
            logger.info(f'Opening in Nautilus: {file_path}')
            subprocess.check_call(["nautilus", "--browser", file_path])
        else:
            self.con.parent_win.show_error_msg('Cannot open file in Nautilus', f'File not found: {file_path}')

    # --- END ACTIONS ---
