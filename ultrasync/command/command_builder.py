import collections
import logging
from typing import Callable, Deque, Dict, Iterable, List, Optional, Tuple

import treelib

from command.change_action import ChangeAction, ChangeType
from command.command_interface import Command, CommandBatch
from command.command_impl import CopyFileLocallyCommand, CreateGDriveFolderCommand, \
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
        self._build_dict: Dict[ChangeType, Dict[str, Callable]] = self._populate_build_dict()

    def build_command_batch(self, change_tree: CategoryDisplayTree = None, delete_list: List[DisplayNode] = None) -> CommandBatch:
        """Builds a dependency tree consisting of commands, each of which correspond to one of the relevant nodes in the
        change tree, or alternatively, the delete_list"""
        command_tree = treelib.Tree()
        # As usual, root is not used for much. All children of root have no dependencies:
        cmd_root = command_tree.create_node(identifier=ROOT_UID, parent=None, data=None)

        if change_tree:
            self._populate_cmd_tree_from_change_tree(change_tree, command_tree, cmd_root)
        elif delete_list:
            self._populate_cmd_tree_from_delete_list(delete_list, command_tree, cmd_root)
        else:
            raise RuntimeError('Neither change_tree nor delete_list specified!')

        return CommandBatch(self._uid_generator.next_uid(), command_tree)

    # From delete_list
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _create_change_action(self, change_type: ChangeType, src_node: DisplayNode, dst_node: DisplayNode = None):
        if src_node:
            src_uid = src_node.uid
        else:
            src_uid = None

        if dst_node:
            dst_uid = dst_node.uid
        else:
            dst_uid = None

        action_uid = self._uid_generator.next_uid()
        return ChangeAction(change_type=change_type, action_uid=action_uid, src_uid=src_uid, dst_uid=dst_uid)

    def _populate_cmd_tree_from_delete_list(self, delete_list: List[DisplayNode], command_tree: treelib.Tree, cmd_root):
        logger.debug(f'Building command batch from delete_list of size {len(delete_list)}')
        # Deletes are much simpler than other change types. We delete files one by one, with no dependencies needed
        for node in delete_list:
            change_action = self._create_change_action(ChangeType.RM, src_node=node)
            cmd: Command = self._build_command(node, change_action)
            assert cmd is not None
            command_tree.add_node(node=cmd, parent=cmd_root)

    # From change_tree
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def _populate_cmd_tree_from_change_tree(self, change_tree: CategoryDisplayTree, command_tree: treelib.Tree, cmd_root):
        stack: Deque[Tuple[treelib.Node, DisplayNode]] = collections.deque()
        src_children: Iterable[DisplayNode] = change_tree.get_children_for_root()
        for change_node in src_children:
            stack.append((cmd_root, change_node))

        while len(stack) > 0:
            cmd_parent, change_node = stack.popleft()

            change_action = change_tree.get_change_action_for_node(change_node)
            if change_action:
                cmd: Command = self._build_command(change_node, change_action)
                assert cmd is not None
                command_tree.add_node(node=cmd, parent=cmd_parent)

                if change_node.is_dir():
                    # added folder creates extra level of dependency:
                    cmd_parent = cmd

            change_children = change_tree.get_children(change_node)
            for change_child in change_children:
                stack.append((cmd_parent, change_child))

    def _populate_build_dict(self):
        """Every command has an associated target node and a ChangeAction. Many commands also have an associated source node."""
        build_dict = {}
        build_dict[ChangeType.MKDIR] = {
            GD: lambda uid, change, tgt, src: CreateGDriveFolderCommand(tgt_node=tgt, change_action=change, uid=uid),
            LO: lambda uid, change, tgt, src: CreatLocalDirCommand(tgt_node=tgt, change_action=change, uid=uid)
        }

        build_dict[ChangeType.CP] = {
            LO_LO: lambda uid, change, tgt, src: CopyFileLocallyCommand(uid, change, tgt_node=tgt, src_node=src, overwrite=False),

            LO_GD: lambda uid, change, tgt, src: UploadToGDriveCommand(uid, change, tgt_node=tgt, src_node=src),

            GD_LO: lambda uid, change, tgt, src, overwrite: DownloadFromGDriveCommand(uid, change, tgt_node=tgt, src_node=src, overwrite=overwrite)
        }

        build_dict[ChangeType.MV] = {
            LO_LO: lambda uid, change, tgt, src: MoveFileLocallyCommand(uid, change, tgt_node=tgt, src_node=src),

            GD_GD: lambda uid, change, tgt, src: MoveFileGDriveCommand(uid, change, tgt_node=tgt, src_node=src),

            LO_GD: lambda uid, change, tgt, src: UploadToGDriveCommand(uid, change, tgt_node=tgt, src_node=src),

            GD_LO: lambda uid, change, tgt, src, overwrite: DownloadFromGDriveCommand(uid, change, tgt_node=tgt, src_node=src, overwrite=overwrite)
        }

        build_dict[ChangeType.RM] = {
            LO: lambda uid, change, tgt, src_node: DeleteLocalFileCommand(uid, change, tgt_node=tgt, to_trash=True, delete_empty_parent=True),

            GD: lambda uid, change, tgt, src_node: DeleteGDriveFileCommand(uid, change, tgt_node=tgt, to_trash=True, delete_empty_parent=True)
        }

        build_dict[ChangeType.UP] = {
            LO_LO: lambda uid, change, tgt, src: CopyFileLocallyCommand(uid, change, tgt_node=tgt, src_node=src, overwrite=True),

            LO_GD: lambda uid, change, tgt, src: UploadToGDriveCommand(uid, change, tgt_node=tgt, src_node=src),

            GD_LO: lambda uid, change, tgt, src, overwrite: DownloadFromGDriveCommand(uid, change, tgt_node=tgt, src_node=src, overwrite=overwrite),
        }
        return build_dict

    def _build_command(self, tgt_node: DisplayNode, change_action: ChangeAction):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Building command for ChangeAction={change_action},  tgt_node={tgt_node}')

        if change_action.dst_uid:
            # Src AND Dst:
            assert change_action.dst_uid == tgt_node.uid, f'For tgt_node={tgt_node}, change_action={change_action}'
            # TODO: [improvement] look up MD5 for src_node and use a closer node

            src_node: Optional[DisplayNode] = self._cache_manager.get_item_for_uid(change_action.src_uid)
            if not src_node:
                raise RuntimeError(f'Could not find referenced src_node in cache with uid={change_action.src_uid} (dst={tgt_node})')

            tree_type_key = _make_key(src_node.node_identifier.tree_type, tgt_node.node_identifier.tree_type)
        else:
            # Only Src:
            assert change_action.src_uid == tgt_node.uid, f'For tgt_node={tgt_node}, change_action={change_action}'
            src_node = None

            tree_type_key = _make_key(tgt_node.node_identifier.tree_type)

        tree_type_dict = self._build_dict.get(change_action.change_type)
        if not tree_type_dict:
            raise RuntimeError(f'Unrecognized ChangeType: {change_action.change_type}')

        build_func = tree_type_dict.get(tree_type_key, None)
        if not build_func:
            raise RuntimeError(f'Bad tree type(s): {tree_type_key}, for ChangeType "{change_action.change_type.name}"')
        uid = self._uid_generator.next_uid()
        return build_func(uid, change_action, tgt_node, src_node)

