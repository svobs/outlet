import logging
import logging
import os
import re
import subprocess
from typing import List, Optional

import gi

from model.display_node import CategoryNode, DisplayNode
from model.planning_node import FileDecoratorNode

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, GObject

logger = logging.getLogger(__name__)

DATE_REGEX = r'^[\d]{4}(\-[\d]{2})?(-[\d]{2})?'

# CLASS ContextActionsLocaldisk
# ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟


class ContextActionsLocaldisk:
    def __init__(self, controller):
        self.con = controller

    def build_context_menu_multiple(self, selected_items: List[DisplayNode]) -> Optional[Gtk.Menu]:
        menu = Gtk.Menu()

        item = Gtk.MenuItem(label=f'Use EXIFTool on dirs')
        item.connect('activate', lambda menu_item: self.call_exiftool_list(selected_items))
        menu.append(item)

        menu.show_all()
        return menu

    def build_context_menu(self, tree_path: Gtk.TreePath, node_data: DisplayNode) -> Optional[Gtk.Menu]:
        """Dynamic context menu (right-click on tree item)"""

        if not node_data.has_path():
            # 'Loading' node, 'Empty' node, etc.
            return

        menu = Gtk.Menu()

        if isinstance(node_data, FileDecoratorNode):
            # TODO: better handling of GDrive paths
            full_path = node_data.original_full_path
        else:
            full_path = self.con.get_tree().get_full_path_for_item(node_data)
        file_name = os.path.basename(full_path)

        is_category_node = type(node_data) == CategoryNode
        file_exists = os.path.exists(full_path)

        item = Gtk.MenuItem(label='')
        label = item.get_child()
        full_path_display = GObject.markup_escape_text(full_path)
        label.set_markup(f'<i>{full_path_display}</i>')
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

            match = re.match(DATE_REGEX, file_name)
            if match:
                item = Gtk.MenuItem(label=f'Use EXIFTool on dir')
                item.connect('activate', lambda menu_item: self.call_exiftool(full_path))
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

    # ACTIONS begin
    # ⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟⮟

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

    def expand_all(self, menu_item, tree_path):
        self.con.display_strategy.display_all(tree_path)

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
            root_path = self.con.get_tree().get_root_path()
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

    def call_exiftool_list(self, data_node_list: List[DisplayNode]):
        for item in data_node_list:
            self.call_exiftool(item.full_path)

    def call_exiftool(self, file_path):
        """exiftool -AllDates="2001:01:01 12:00:00" *
        exiftool -Comment="Hawaii" {target_dir}
        find . -name "*jpg_original" -exec rm -fv {} \;
        """
        if not os.path.exists(file_path):
            self.con.parent_win.show_error_msg(f'Cannot manipulate dir', f'Dir not found: {file_path}')
            return
        if not os.path.isdir(file_path):
            self.con.parent_win.show_error_msg(f'Cannot manipulate dir', f'Not a dir: {file_path}')
            return
        dir_name = os.path.basename(file_path)
        tokens = dir_name.split(' ', 1)
        comment_to_set = None
        if len(tokens) > 1:
            assert not len(tokens) > 2, f'Length of tokens is {len(tokens)}: "{file_path}"'
            comment_to_set = tokens[1]
        date_to_set = tokens[0]
        if not re.fullmatch(DATE_REGEX + '$', date_to_set):
            raise RuntimeError(f'Unexpected date pattern: {tokens[0]}')
        if len(date_to_set) == 10:
            # good, whole date. Just to be sure, replace all dashes with colons
            pass
        elif len(date_to_set) == 7:
            # only year + month found. Add default day
            date_to_set += ':01'
        elif len(date_to_set) == 4:
            # only year found. Add default day
            date_to_set += ':01:01'
        date_to_set = date_to_set.replace('-', ':')

        logger.info(f'Calling exiftool for: {file_path}')
        args = ["exiftool", f'-AllDates="{date_to_set} 12:00:00"']
        if comment_to_set:
            args.append(f'-Comment="{comment_to_set}"')
        args.append(file_path)
        subprocess.check_call(args)

        list_original_files = [f.path for f in os.scandir(file_path) if not f.is_dir() and f.path.endswith('.jpg_original')]
        for file in list_original_files:
            logger.debug(f'Removing file: {file}')
            os.remove(file)

    # ⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝⮝
    # ACTIONS end
