import collections
from typing import Deque, Iterable, List, Tuple

import treelib

from command.command import Command, CommandPlan, CopyFileLocallyCommand, CreateGDriveFolderCommand, \
    DeleteGDriveFileCommand, DeleteLocalFileCommand, \
    DownloadFromGDriveCommand, \
    MoveFileGDriveCommand, \
    MoveFileLocallyCommand, \
    UploadToGDriveCommand
from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from index.uid_generator import ROOT_UID
from model.category import Category
from model.display_node import DisplayNode
from model.goog_node import FolderToAdd
from model.planning_node import FileToAdd, FileToMove, FileToUpdate
from ui.tree.category_display_tree import CategoryDisplayTree


class CommandBuilder:
    def __init__(self, application):
        self._uid_generator = application.uid_generator
        self._cache_manager = application.cache_manager

    def build_command_plan(self, change_tree: CategoryDisplayTree) -> CommandPlan:
        command_tree = treelib.Tree()
        # As usual, root is not used for much:
        cmd_root = command_tree.create_node(identifier=ROOT_UID, parent=None, data=None)

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

                if isinstance(cmd, CreateGDriveFolderCommand):
                    # added GDrive folder creates extra level of dependency:
                    dst_parent = cmd

            src_children = change_tree.get_children(src_node.identifier)
            for child in src_children:
                stack.append((dst_parent, child))

        return CommandPlan(self._uid_generator.get_new_uid(), command_tree)


def _make_command(node: DisplayNode, uid_generator):
    # FIXME: support directories and folders!
    tree_type: int = node.node_identifier.tree_type
    if node.category == Category.Added:
        if isinstance(node, FolderToAdd):
            return CreateGDriveFolderCommand(model_obj=node, uid=uid_generator.get_new_uid())
        assert isinstance(node, FileToAdd)
        orig_tree_type: int = node.src_node.node_identifier.tree_type
        if orig_tree_type == tree_type:
            if tree_type == OBJ_TYPE_LOCAL_DISK:
                return CopyFileLocallyCommand(model_obj=node, uid=uid_generator.get_new_uid())
            elif tree_type == OBJ_TYPE_GDRIVE:
                raise RuntimeError(f'Bad tree type: {tree_type}')
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')
        elif orig_tree_type == OBJ_TYPE_LOCAL_DISK and tree_type == OBJ_TYPE_GDRIVE:
            return UploadToGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        elif orig_tree_type == OBJ_TYPE_GDRIVE and tree_type == OBJ_TYPE_LOCAL_DISK:
            return DownloadFromGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        else:
            raise RuntimeError(f'Bad tree type(s): src={orig_tree_type},dst={tree_type}')
    elif node.category == Category.Moved:
        assert isinstance(node, FileToMove)
        orig_tree_type: int = node.src_node.node_identifier.tree_type
        if orig_tree_type == tree_type:
            if tree_type == OBJ_TYPE_LOCAL_DISK:
                return MoveFileLocallyCommand(model_obj=node, uid=uid_generator.get_new_uid())
            elif tree_type == OBJ_TYPE_GDRIVE:
                return MoveFileGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')
        elif orig_tree_type == OBJ_TYPE_LOCAL_DISK and tree_type == OBJ_TYPE_GDRIVE:
            return UploadToGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        elif orig_tree_type == OBJ_TYPE_GDRIVE and tree_type == OBJ_TYPE_LOCAL_DISK:
            return DownloadFromGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid())
        else:
            raise RuntimeError(f'Bad tree type(s): src={orig_tree_type}, dst={tree_type}')
    elif node.category == Category.Deleted:
        if tree_type == OBJ_TYPE_LOCAL_DISK:
            return DeleteLocalFileCommand(model_obj=node, uid=uid_generator.get_new_uid())
        elif tree_type == OBJ_TYPE_GDRIVE:
            return DeleteGDriveFileCommand(model_obj=node, uid=uid_generator.get_new_uid())
        else:
            raise RuntimeError(f'Bad tree type: {tree_type}')
    elif node.category == Category.Updated:
        assert isinstance(node, FileToUpdate)
        orig_tree_type = node.src_node.node_identifier.tree_type
        if orig_tree_type == tree_type:
            if tree_type == OBJ_TYPE_LOCAL_DISK:
                return CopyFileLocallyCommand(model_obj=node, uid=uid_generator.get_new_uid(), overwrite=True)
            elif tree_type == OBJ_TYPE_GDRIVE:
                raise RuntimeError(f'Bad tree type: {tree_type}')
            else:
                raise RuntimeError(f'Bad tree type: {tree_type}')
        elif orig_tree_type == OBJ_TYPE_LOCAL_DISK and tree_type == OBJ_TYPE_GDRIVE:
            return UploadToGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid(), overwrite=True)
        elif orig_tree_type == OBJ_TYPE_GDRIVE and tree_type == OBJ_TYPE_LOCAL_DISK:
            return DownloadFromGDriveCommand(model_obj=node, uid=uid_generator.get_new_uid(), overwrite=True)
        else:
            raise RuntimeError(f'Bad tree type(s): src={orig_tree_type},dst={tree_type}')
    else:
        raise RuntimeError(f'Unsupported category: {node.category}')
