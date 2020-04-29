import logging
import os

from treelib import Tree
import file_util
from model.category import Category
from model.display_node import CategoryNode, DirNode
from model.fmeta_tree import FMetaTree

logger = logging.getLogger(__name__)


def build_category_tree(fmeta_tree: FMetaTree, root_node: CategoryNode) -> Tree:
    """
    Builds a tree out of the flat file set.
    Args:
        fmeta_tree: source tree
        root_node: a display node representing the category

    Returns:
        change tree
    """
    # The change set in tree form
    change_tree = Tree()  # from treelib

    category: Category = root_node.category
    fmeta_list = fmeta_tree.get_for_cat(category)
    set_len = len(fmeta_list)

    logger.info(f'Building change trees for category {category.name} with {set_len} files...')

    root = change_tree.create_node(tag=f'{category.name} ({set_len} files)',
                                   identifier=root_node.full_path, data=root_node)   # root
    for fmeta in fmeta_list:
        dirs_str, file_name = os.path.split(fmeta.get_relative_path(fmeta_tree.root_path))
        # nid == Node ID == directory name
        nid = root_node.full_path
        parent = root
        # logger.debug(f'Adding root file "{fmeta.full_path}" to dir "{parent.data.full_path}"')
        parent.data.add_meta(fmeta)
        if dirs_str != '':
            # Create a node for each ancestor dir (path segment)
            path_segments = file_util.split_path(dirs_str)
            for dir_name in path_segments:
                nid = os.path.join(nid, dir_name)
                child = change_tree.get_node(nid=nid)
                if child is None:
                    dir_full_path = os.path.join(fmeta_tree.root_path, nid)
                    # logger.debug(f'Creating dir node: nid={nid}')
                    child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=DirNode(dir_full_path, category))
                parent = child
                # logger.debug(f'Adding file meta from nid="{fmeta.full_path}" to dir node {parent.data.full_path}"')
                parent.data.add_meta(fmeta)
        nid = os.path.join(nid, file_name)
        # logger.debug(f'Creating file node: nid={nid}')
        change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=fmeta)

    return change_tree

