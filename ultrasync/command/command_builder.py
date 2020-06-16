import collections
import logging
from typing import Deque, Iterable, List, Tuple

import treelib
logger = logging.getLogger(__name__)

from command.command_interface import Command, CommandBatch
from command.command_impl import CopyFileLocallyCommand, CreateGDriveFolderCommand, \
    CreatLocalDirCommand, DeleteGDriveFileCommand, DeleteLocalFileCommand, \
    DownloadFromGDriveCommand, \
    MoveFileGDriveCommand, \
    MoveFileLocallyCommand, \
    UploadToGDriveCommand
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from index.uid_generator import ROOT_UID
from model.category import Category
from model.display_node import DisplayNode
from model.goog_node import FolderToAdd
from model.planning_node import FileToAdd, FileToMove, FileToUpdate, LocalDirToAdd, PlanningNode
from model.subtree_snapshot import SubtreeSnapshot


# CLASS CommandBuilder
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CommandBuilder:
    def __init__(self, application):
        self._uid_generator = application.uid_generator
        self._cache_manager = application.cache_manager

    def build_command_batch(self, change_tree: SubtreeSnapshot = None, delete_list: List[DisplayNode] = None) -> CommandBatch:
        """Builds a dependency tree consisting of commands, each of which correspond to one of the relevant nodes in the
        change tree, or alternatively, the delete_list"""
        command_tree = treelib.Tree()
        # As usual, root is not used for much. All children of root have no dependencies:
        cmd_root = command_tree.create_node(identifier=ROOT_UID, parent=None, data=None)

        if change_tree:
            stack: Deque[Tuple[treelib.Node, DisplayNode]] = collections.deque()
            src_children: Iterable[DisplayNode] = change_tree.get_children_for_root()
            for child in src_children:
                stack.append((cmd_root, child))

            while len(stack) > 0:
                dst_parent, src_node = stack.popleft()

                # Don't even bother to create commands for display-only nodes such as DirNodes, etc
                if not src_node.is_just_fluff():
                    cmd: Command = _make_command(src_node, self._uid_generator)
                    assert cmd is not None
                    command_tree.add_node(node=cmd, parent=dst_parent)

                    if isinstance(cmd, CreateGDriveFolderCommand) or isinstance(cmd, CreatLocalDirCommand):
                        # added folder creates extra level of dependency:
                        dst_parent = cmd

                src_children = change_tree.get_children(src_node)
                for child in src_children:
                    stack.append((dst_parent, child))
        elif delete_list:
            logger.debug(f'Building command plan from delete_list of size {len(delete_list)}')
            # Deletes are much simpler than other change types. We delete files one by one, with no dependencies needed
            for node in delete_list:
                assert not node.is_just_fluff() and not isinstance(node, PlanningNode) and node.category == Category.Deleted
                cmd: Command = _make_command(node, self._uid_generator)
                assert cmd is not None
                command_tree.add_node(node=cmd, parent=cmd_root)
        else:
            raise RuntimeError('Neither change_tree nor delete_list specified!')

        return CommandBatch(self._uid_generator.get_new_uid(), command_tree)


def _make_command(node: DisplayNode, uid_generator):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f'Building command for node: {node}')

    tree_type: int = node.node_identifier.tree_type
    if node.category == Category.Added:
        if isinstance(node, FolderToAdd):
            return CreateGDriveFolderCommand(model_obj=node, uid=uid_generator.get_new_uid())
        if isinstance(node, LocalDirToAdd):
            return CreatLocalDirCommand(model_obj=node, uid=uid_generator.get_new_uid())
        assert isinstance(node, FileToAdd)
        orig_tree_type: int = node.src_node.node_identifier.tree_type
        if orig_tree_type == tree_type:
            if tree_type == TREE_TYPE_LOCAL_DISK:
                return CopyFileLocallyCommand(model_obj=node, uid=uid_generator.get_new_uid())
            elif tree_type == TREE_TYPE_GDRIVE:
                raise RuntimeError(f'Bad tree type: {tree_type}')
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')
        elif orig_tree_type == TREE_TYPE_LOCAL_DISK and tree_type == TREE_TYPE_GDRIVE:
            return UploadToGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        elif orig_tree_type == TREE_TYPE_GDRIVE and tree_type == TREE_TYPE_LOCAL_DISK:
            return DownloadFromGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        else:
            raise RuntimeError(f'Bad tree type(s): src={orig_tree_type},dst={tree_type}')
    elif node.category == Category.Moved:
        assert isinstance(node, FileToMove)
        orig_tree_type: int = node.src_node.node_identifier.tree_type
        if orig_tree_type == tree_type:
            if tree_type == TREE_TYPE_LOCAL_DISK:
                return MoveFileLocallyCommand(model_obj=node, uid=uid_generator.get_new_uid())
            elif tree_type == TREE_TYPE_GDRIVE:
                return MoveFileGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')
        elif orig_tree_type == TREE_TYPE_LOCAL_DISK and tree_type == TREE_TYPE_GDRIVE:
            return UploadToGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        elif orig_tree_type == TREE_TYPE_GDRIVE and tree_type == TREE_TYPE_LOCAL_DISK:
            return DownloadFromGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        else:
            raise RuntimeError(f'Bad tree type(s): src={orig_tree_type}, dst={tree_type}')
    elif node.category == Category.Deleted:
        if tree_type == TREE_TYPE_LOCAL_DISK:
            return DeleteLocalFileCommand(model_obj=node, uid=uid_generator.get_new_uid(), to_trash=False, delete_empty_parent=True)
        elif tree_type == TREE_TYPE_GDRIVE:
            return DeleteGDriveFileCommand(model_obj=node, uid=uid_generator.get_new_uid(), to_trash=True, delete_empty_parent=False)
        else:
            raise RuntimeError(f'Bad tree type: {tree_type}')
    elif node.category == Category.Updated:
        assert isinstance(node, FileToUpdate)
        orig_tree_type = node.src_node.node_identifier.tree_type
        if orig_tree_type == tree_type:
            if tree_type == TREE_TYPE_LOCAL_DISK:
                return CopyFileLocallyCommand(model_obj=node, uid=uid_generator.get_new_uid(), overwrite=True)
            elif tree_type == TREE_TYPE_GDRIVE:
                raise RuntimeError(f'Bad tree type: {tree_type}')
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')
        elif orig_tree_type == TREE_TYPE_LOCAL_DISK and tree_type == TREE_TYPE_GDRIVE:
            return UploadToGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid(), overwrite=True)
        elif orig_tree_type == TREE_TYPE_GDRIVE and tree_type == TREE_TYPE_LOCAL_DISK:
            return DownloadFromGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid(), overwrite=True)
        else:
            raise RuntimeError(f'Bad tree type(s): src={orig_tree_type},dst={tree_type}')
    else:
        raise RuntimeError(f'Unsupported category: {node.category}')
