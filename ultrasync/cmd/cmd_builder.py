import collections
import logging
from typing import Callable, Deque, Dict, Iterable, List, Tuple

import treelib

from model.change_action import ChangeAction, ChangeType
from cmd.cmd_interface import Command, CommandBatch
from cmd.cmd_impl import CopyFileLocallyCommand, CreateGDriveFolderCommand, \
    CreatLocalDirCommand, DeleteGDriveFileCommand, DeleteLocalFileCommand, \
    DownloadFromGDriveCommand, \
    MoveFileGDriveCommand, \
    MoveFileLocallyCommand, \
    UploadToGDriveCommand
from constants import ROOT_UID, TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.display_node import DisplayNode
from ui.tree.category_display_tree import CategoryDisplayTree

logger = logging.getLogger(__name__)


def _make_key(tree_type_src, tree_type_dst=None):
    if tree_type_dst:
        return f'{tree_type_src}->{tree_type_dst}'
    return f'{tree_type_src}'


LO = _make_key(TREE_TYPE_LOCAL_DISK)
GD = _make_key(TREE_TYPE_GDRIVE)
LO_LO = _make_key(TREE_TYPE_LOCAL_DISK, TREE_TYPE_LOCAL_DISK)
GD_GD = _make_key(TREE_TYPE_GDRIVE, TREE_TYPE_GDRIVE)
LO_GD = _make_key(TREE_TYPE_LOCAL_DISK, TREE_TYPE_GDRIVE)
GD_LO = _make_key(TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK)


# CLASS CommandBuilder
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CommandBuilder:
    def __init__(self, application):
        self._uid_generator = application.uid_generator
        self._cache_manager = application.cache_manager
        self._build_dict: Dict[ChangeType, Dict[str, Callable]] = _populate_build_dict()

    def build_command_batch(self, change_list: Iterable[ChangeAction]) -> CommandBatch:
        """Builds a dependency tree consisting of commands, each of which correspond to one of the relevant nodes in the
        change tree, or alternatively, the delete_list"""
        command_tree = treelib.Tree()
        # As usual, root is not used for much. All children of root have no dependencies:
        cmd_root = command_tree.create_node(identifier=ROOT_UID, parent=None, data=None)

        # FIXME!
        return CommandBatch(self._uid_generator.next_uid(), command_tree)

    # From delete_list
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _populate_cmd_tree_from_delete_list(self, delete_list: List[DisplayNode], command_tree: treelib.Tree, cmd_root):
        logger.debug(f'Building command batch from delete_list of size {len(delete_list)}')
        # Deletes are much simpler than other change types. We delete files one by one, with no dependencies needed
        for node in delete_list:
            change_action = self._create_change_action(ChangeType.RM, src_node=node)
            cmd: Command = self._build_command(change_action)
            assert cmd is not None
            command_tree.add_node(node=cmd, parent=cmd_root)

    # From change_tree
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _populate_cmd_tree_from_change_tree(self, change_tree: CategoryDisplayTree, command_tree: treelib.Tree, cmd_root):
        stack: Deque[Tuple[treelib.Node, DisplayNode]] = collections.deque()
        src_children: Iterable[DisplayNode] = change_tree.get_children_for_root()
        for change_node in src_children:
            stack.append((cmd_root, change_node))

        # FIXME: need to rework this logic

        while len(stack) > 0:
            cmd_parent, change_node = stack.popleft()

            change_action = change_tree.get_change_action_for_node(change_node)
            if change_action:
                cmd: Command = self._build_command(change_action)
                assert cmd is not None
                command_tree.add_node(node=cmd, parent=cmd_parent)

                if change_node.is_dir():
                    # added folder creates extra level of dependency:
                    cmd_parent = cmd

            change_children = change_tree.get_children(change_node)
            for change_child in change_children:
                stack.append((cmd_parent, change_child))

    def _build_command(self, change_action: ChangeAction):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Building command for ChangeAction={change_action}')

        # TODO: [improvement] look up MD5 for src_node and use a closer node

        if change_action.dst_node:
            # Src AND Dst:
            tree_type_key = _make_key(change_action.src_node.node_identifier.tree_type, change_action.dst_node.node_identifier.tree_type)
        else:
            # Only Src:
            tree_type_key = _make_key(change_action.src_node.node_identifier.tree_type)

        tree_type_dict = self._build_dict.get(change_action.change_type)
        if not tree_type_dict:
            raise RuntimeError(f'Unrecognized ChangeType: {change_action.change_type}')

        build_func = tree_type_dict.get(tree_type_key, None)
        if not build_func:
            raise RuntimeError(f'Bad tree type(s): {tree_type_key}, for ChangeType "{change_action.change_type.name}"')
        uid = self._uid_generator.next_uid()
        return build_func(uid, change_action)


def _populate_build_dict():
    """Every command has an associated target node and a ChangeAction. Many commands also have an associated source node."""
    return {ChangeType.MKDIR: {
        GD: lambda uid, change: CreateGDriveFolderCommand(change_action=change, uid=uid),
        LO: lambda uid, change: CreatLocalDirCommand(change_action=change, uid=uid)
    }, ChangeType.CP: {
        LO_LO: lambda uid, change: CopyFileLocallyCommand(uid, change, overwrite=False),

        LO_GD: lambda uid, change: UploadToGDriveCommand(uid, change, overwrite=False),

        GD_LO: lambda uid, change: DownloadFromGDriveCommand(uid, change, overwrite=False)
    }, ChangeType.MV: {
        LO_LO: lambda uid, change: MoveFileLocallyCommand(uid, change),

        GD_GD: lambda uid, change: MoveFileGDriveCommand(uid, change),

        LO_GD: lambda uid, change: UploadToGDriveCommand(uid, change, overwrite=False),

        GD_LO: lambda uid, change: DownloadFromGDriveCommand(uid, change, overwrite=False)
    }, ChangeType.RM: {
        LO: lambda uid, change: DeleteLocalFileCommand(uid, change, to_trash=True, delete_empty_parent=True),

        GD: lambda uid, change: DeleteGDriveFileCommand(uid, change, to_trash=True, delete_empty_parent=True)
    }, ChangeType.UP: {
        LO_LO: lambda uid, change: CopyFileLocallyCommand(uid, change, overwrite=True),

        LO_GD: lambda uid, change: UploadToGDriveCommand(uid, change, overwrite=True),

        GD_LO: lambda uid, change: DownloadFromGDriveCommand(uid, change, overwrite=True),
    }}
