"""
Populates the DiffTree's model from a given FMetaTree.
"""
from datetime import datetime
import os
import humanfriendly
import logging
from stopwatch import Stopwatch
from treelib import Tree
import file_util
from fmeta.fmeta import FMeta, FMetaTree, Category
from ui.diff_tree_nodes import DirNode, CategoryNode
import gi
gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk, Gdk, GdkPixbuf

logger = logging.getLogger(__name__)


def _build_category_change_tree(fmeta_list, category):
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
    if set_len > 0:
        logger.info(f'Building change trees for category {category.name} with {set_len} files...')

        root = change_tree.create_node(tag=f'{category.name} ({set_len} files)', identifier='', data=CategoryNode(category))   # root
        for fmeta in fmeta_list:
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


def _append_dir_node(diff_tree, tree_iter, dir_name, node_data):
    """Appends a dir or cat node to the model"""
    row_values = []
    if diff_tree.editable:
        row_values.append(False)  # Checked
        row_values.append(False)  # Inconsistent
    row_values.append('folder')  # Icon
    row_values.append(dir_name)  # Name
    if not diff_tree.use_dir_tree:
        row_values.append(None)  # Directory
    num_bytes_str = humanfriendly.format_size(node_data.size_bytes)
    row_values.append(num_bytes_str)  # Size
    row_values.append(None)  # Modify Date
    if diff_tree.show_change_ts:
        row_values.append(None)  # Modify Date
    row_values.append(node_data)  # Data

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


def _append_to_model(diff_tree, category, change_tree):
    def append_recursively(tree_iter, node):
        # Do a DFS of the change tree and populate the UI tree along the way
        if isinstance(node.data, DirNode):
            # Is dir
            tree_iter = _append_dir_node(diff_tree, tree_iter, node.tag, node.data)
            for child in change_tree.children(node.identifier):
                append_recursively(tree_iter, child)
        else:
            _append_fmeta_node(diff_tree, tree_iter, node.tag, node.data, category)

    if change_tree.size(1) > 0:
        #logger.debug(f'Appending category: {category.name}')
        root = change_tree.get_node('')
        append_recursively(None, root)


def _populate_category(diff_tree, category: Category, fmeta_list):
    # Build fake tree for category:
    stopwatch = Stopwatch()
    change_tree = _build_category_change_tree(fmeta_list, category)
    stopwatch.stop()
    logger.debug(f'Faux tree built for "{category.name}" in: {stopwatch}')

    stopwatch = Stopwatch()
    _append_to_model(diff_tree, category, change_tree)
    stopwatch.stop()
    logger.debug(f'TreeStore populated for "{category.name}" in: {stopwatch}')


def _set_expand_states_from_config(diff_tree):
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


def repopulate_diff_tree(diff_tree):
    """
    Populates the given DiffTree using categories as the topmost elements.
    TODO: looks like we'll have to implement lazy loading to speed things up...
    Args:
        diff_tree: the DiffTree widget

    """
    logger.debug(f'Repopulating diff tree "{diff_tree.tree_id}"')

    # Wipe out existing items:
    diff_tree.model.clear()

    # Detach model before insert.
    # The docs say to do this for speed, but it doesn't seem to change things:
    diff_tree.treeview.set_model(None)

    fmeta_tree = diff_tree.data_source.get_fmeta_tree()

    for category in [Category.Added,
                     Category.Deleted,
                     Category.Moved,
                     Category.Updated,
                     Category.Ignored]:
        _populate_category(diff_tree, category, fmeta_tree.get_for_cat(category))

    # Re-attach model:
    GLib.idle_add(lambda: diff_tree.treeview.set_model(diff_tree.model))

    # Restore user prefs for expanded nodes:
    GLib.idle_add(_set_expand_states_from_config, diff_tree)

    logger.debug(f'Done repopulating diff tree "{diff_tree.tree_id}"')
