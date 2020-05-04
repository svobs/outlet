import logging
import os

from treelib import Tree
import file_util
from model.category import Category
from model.display_node import CategoryNode, DirNode
from model.subtree_snapshot import SubtreeSnapshot

logger = logging.getLogger(__name__)


def build_category_tree(source_tree: SubtreeSnapshot, root_node: CategoryNode) -> Tree:
    """
    Builds a tree out of the flat file set.
    Args:
        source_tree: source tree
        root_node: a display node representing the category

    Returns:
        change tree
    """
    # The change set in tree form
    change_tree = Tree()  # from treelib

    category: Category = root_node.category
    cat_item_list = source_tree.get_for_cat(category)
    set_len = len(cat_item_list)

    logger.debug(f'Building change trees for category {category.name} with {set_len} files...')
    root_node_id = ''

    root = change_tree.create_node(tag=f'{category.name} ({set_len} files)',
                                   identifier=root_node_id, data=root_node)   # root
    for item in cat_item_list:
        if item.is_dir():
            # Skip any actual directories we encounter. We won't use them for our display, because:
            # (1) each category has a logically different dir with the same ID, and let's not get confused, and
            # (2) there's nothing for us in these objects from a display perspective. The name can be inferred
            # from each file's path, and we don't want to display empty dirs when there's no file of that category
            continue
        dirs_str, file_name = os.path.split(item.get_relative_path(source_tree))
        # nid == Node ID == directory name
        nid = root_node_id
        parent = root
        logger.debug(f'Adding root file "{item.display_id.id_string}" to dir "{parent.data.full_path}"')
        parent.data.add_meta_emtrics(item)
        if dirs_str != '':
            # Create a node for each ancestor dir (path segment)
            path_segments = file_util.split_path(dirs_str)
            for dir_name in path_segments:
                nid = os.path.join(nid, dir_name)
                child = change_tree.get_node(nid=nid)
                if child is None:
                    dir_full_path = os.path.join(source_tree.root_path, nid)
                    logger.debug(f'Creating dir node: nid={nid}')
                    dir_node = DirNode(dir_full_path, category)
                    child = change_tree.create_node(tag=dir_name, identifier=nid, parent=parent, data=dir_node)
                parent = child
                # logger.debug(f'Adding file meta from nid="{item.full_path}" to dir node {parent.data.full_path}"')
                assert isinstance(parent.data, DirNode)
                parent.data.add_meta_emtrics(item)
        nid = os.path.join(nid, file_name)
        # logger.debug(f'Creating file node: nid={nid}')
        change_tree.create_node(identifier=nid, tag=file_name, parent=parent, data=item)

    return change_tree

