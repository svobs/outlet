"""
Populates the DiffTree's model from a given FMetaTree.
"""
from datetime import datetime
import os
import humanfriendly
import logging
from treelib import Node, Tree
import file_util
from fmeta.fmeta import FMeta, FMetaTree, Category
from ui.diff_tree_nodes import DirNode, CategoryNode
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf

logger = logging.getLogger(__name__)


def _build_category_change_tree(change_set, category):
    """
    Builds a tree out of the flat change set.
    Args:
        change_set: source tree for category
        cat_name: the category name

    Returns:
        change tree
    """
    # The change set in tree form
    change_tree = Tree()  # from treelib

    set_len = len(change_set)
    if set_len > 0:
        logger.info(f'Building change trees for category {category.name} with {set_len} files...')

        root = change_tree.create_node(tag=f'{category.name} ({set_len} files)', identifier='', data=CategoryNode(category))   # root
        for fmeta in change_set:
            dirs_str, file_name = os.path.split(fmeta.file_path)
            # nid == Node ID == directory name
            nid = ''
            parent = root
            #logger.debug(f'Adding root file "{fmeta.file_path}" to dir "{parent.data.file_path}"')
            parent.data.add_meta(fmeta)
            if dirs_str != '':
                directories = file_util.split_path(dirs_str)
                for dir_name in directories:
                    nid = os.path.join(nid, dir_name)
                    child = change_tree.get_node(nid=nid)
                    if child is None:
                        #logger.debug(f'Creating dir: {nid}')
                        child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=DirNode(nid, category))
                    parent = child
                    #logger.debug(f'Adding file "{fmeta.file_path}" to dir {parent.data.file_path}"')
                    parent.data.add_meta(fmeta)
            nid = os.path.join(nid, file_name)
            #logger.debug(f'Creating file: {nid}')
            change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

    return change_tree


def _append_dir_node(diff_tree, tree_iter, dir_name, dmeta):
    row_values = []
    if diff_tree.editable:
        row_values.append(False)  # Checked
        row_values.append(False)  # Inconsistent
    row_values.append('folder')  # Icon
    row_values.append(dir_name)  # Name
    if not diff_tree.use_dir_tree:
        row_values.append(None)  # Directory
    num_bytes_str = humanfriendly.format_size(dmeta.size_bytes)
    row_values.append(num_bytes_str)  # Size
    row_values.append(None)  # Modify Date
    if diff_tree.show_change_ts:
        row_values.append(None)  # Modify Date
    row_values.append(dmeta)  # Data
    return diff_tree.model.append(tree_iter, row_values)


def _append_fmeta_node(diff_tree, tree_iter, file_name, fmeta: FMeta, category):
    row_values = []

    if diff_tree.editable:
        row_values.append(False)  # Checked
        row_values.append(False)  # Inconsistent
    row_values.append(category.name)  # Icon

    if category == Category.Moved:
        node_name = f'{fmeta.prev_path} -> "{file_name}"'
    else:
        node_name = file_name
    row_values.append(node_name)  # Name

    if not diff_tree.use_dir_tree:
        directory, name = os.path.split(fmeta.file_path)
        row_values.append(directory)  # Directory

    num_bytes_str = humanfriendly.format_size(fmeta.size_bytes)
    row_values.append(num_bytes_str)  # Size

    modify_datetime = datetime.fromtimestamp(fmeta.modify_ts)
    modify_time = modify_datetime.strftime(diff_tree.datetime_format)
    row_values.append(modify_time)  # Modify TS

    if diff_tree.show_change_ts:
        change_datetime = datetime.fromtimestamp(fmeta.change_ts)
        change_time = change_datetime.strftime(diff_tree.datetime_format)
        row_values.append(change_time)  # Change TS

    row_values.append(fmeta)  # Data
    return diff_tree.model.append(tree_iter, row_values)


def _populate_category(diff_tree, category: Category, fmeta_list):
    change_tree = _build_category_change_tree(fmeta_list, category)

    def append_recursively(tree_iter, node):
        # Do a DFS of the change tree and populate the UI tree along the way
        if isinstance(node.data, DirNode):
            # Is dir
            tree_iter = _append_dir_node(diff_tree, tree_iter, node.tag, node.data)
            for child in change_tree.children(node.identifier):
                append_recursively(tree_iter, child)
        else:
            _append_fmeta_node(diff_tree, tree_iter, node.tag, node.data, category)

    def do_on_ui_thread():
        if change_tree.size(1) > 0:
            #logger.debug(f'Appending category: {category.name}')
            root = change_tree.get_node('')
            append_recursively(None, root)

            # Find the category nodes and expand them appropriately
            tree_iter = diff_tree.model.get_iter_first()
            while tree_iter is not None:
                node_data = diff_tree.model[tree_iter][diff_tree.col_num_data]
                if type(node_data) == CategoryNode:
                    cfg_path = diff_tree.get_cat_config_path(node_data.category)
                    is_expand = diff_tree.parent_win.config.get(cfg_path, True)
                    if is_expand:
                        tree_path = diff_tree.model.get_path(tree_iter)
                        diff_tree.treeview.expand_row(path=tree_path, open_all=True)

                tree_iter = diff_tree.model.iter_next(tree_iter)

    GLib.idle_add(do_on_ui_thread)


def repopulate_diff_tree(diff_tree):
    """
    Populates the given DiffTree using categories as the topmost elements
    Args:
        diff_tree:
        fmeta_tree:

    Returns:

    """
    # Wipe out existing items:
    diff_tree.model.clear()

    fmeta_tree = diff_tree.data_source.get_fmeta_tree()
    diff_tree.root_path = fmeta_tree.root_path
    for category in [Category.Added,
                     Category.Deleted,
                     Category.Moved,
                     Category.Updated,
                     Category.Ignored]:
        _populate_category(diff_tree, category, fmeta_tree.get_for_cat(category))
