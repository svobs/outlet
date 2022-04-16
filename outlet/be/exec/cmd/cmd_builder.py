import logging
from typing import Callable, Dict, Optional

from be.exec.cmd.cmd_impl import CopyFileWithinGDriveCommand, CopyFileLocalToLocalCommand, CreateGDriveFolderCommand, \
    CreatLocalDirCommand, DeleteGDriveNodeCommand, DeleteLocalNodeCommand, \
    CopyFileGDriveToLocalCommand, \
    FinishCopyToGDriveFolderCommand, FinishCopyToLocalDirCommand, MoveFileWithinGDriveCommand, \
    MoveFileLocalToLocalCommand, \
    CopyFileLocalToGDriveCommand, StartCopyToGDriveFolderCommand, StartCopyToLocalDirCommand
from be.exec.cmd.cmd_interface import Command
from constants import TreeType
from model.node.node import TNode
from model.user_op import UserOp, UserOpCode

logger = logging.getLogger(__name__)


def _make_key(tree_type_src: TreeType, tree_type_dst: Optional[TreeType] = None, is_same_tree: bool = False):
    if tree_type_dst:
        return f'{tree_type_src}->{"SAME:" if is_same_tree else ""}{tree_type_dst}'
    else:
        return f'{tree_type_src}'


def _make_key_from_node(node_src: TNode, node_dst: Optional[TNode] = None):
    if node_dst:
        is_same_tree = node_src.device_uid == node_dst.device_uid
        return _make_key(node_src.tree_type, node_dst.tree_type, is_same_tree=is_same_tree)
    else:
        return _make_key(node_src.tree_type)


LO = _make_key(TreeType.LOCAL_DISK)
GD = _make_key(TreeType.GDRIVE)
LO_GD = _make_key(TreeType.LOCAL_DISK, TreeType.GDRIVE)
GD_LO = _make_key(TreeType.GDRIVE, TreeType.LOCAL_DISK)
LO_same_LO = _make_key(TreeType.LOCAL_DISK, TreeType.LOCAL_DISK, is_same_tree=True)
LO_different_LO = _make_key(TreeType.LOCAL_DISK, TreeType.LOCAL_DISK, is_same_tree=False)
GD_same_GD = _make_key(TreeType.GDRIVE, TreeType.GDRIVE, is_same_tree=True)
GD_different_GD = _make_key(TreeType.GDRIVE, TreeType.GDRIVE, is_same_tree=False)


class CommandBuilder:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CommandBuilder
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid_generator):
        self._build_dict: Dict[UserOpCode, Dict[str, Callable]] = _populate_build_dict()

    def build_command(self, op: UserOp) -> Command:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Building command for UserOp={op}')

        if op.dst_node:
            # Src AND Dst:
            tree_type_key = _make_key_from_node(op.src_node, op.dst_node)
        else:
            # Only Src:
            tree_type_key = _make_key_from_node(op.src_node)

        tree_type_dict = self._build_dict.get(op.op_type)
        if not tree_type_dict:
            raise RuntimeError(f'Unrecognized UserOpCode: {op.op_type}')

        build_func = tree_type_dict.get(tree_type_key, None)
        if not build_func:
            raise RuntimeError(f'Bad tree type(s): {tree_type_key}, for UserOpCode "{op.op_type.name}"')
        return build_func(op)


def _fail(change, key):
    raise RuntimeError(f'No command for {key} & {change}')


def _populate_build_dict():
    """Every command has an associated target node and a UserOp. Many commands also have an associated source node."""
    return {
        UserOpCode.RM: {
            LO: lambda change: DeleteLocalNodeCommand(change, to_trash=False),
            GD: lambda change: DeleteGDriveNodeCommand(change, to_trash=False)
        },
        UserOpCode.MKDIR: {
            GD: lambda change: CreateGDriveFolderCommand(op=change),
            LO: lambda change: CreatLocalDirCommand(op=change)
        },
        UserOpCode.UNLINK: {
            # TODO unlink support
            GD: lambda change: _fail(change, GD),
            LO: lambda change: _fail(change, LO),
        },

        UserOpCode.CP: {
            LO_same_LO: lambda change: CopyFileLocalToLocalCommand(change, overwrite=False),
            GD_same_GD: lambda change: CopyFileWithinGDriveCommand(change, overwrite=False),
            LO_GD: lambda change: CopyFileLocalToGDriveCommand(change, overwrite=False, delete_src_node_after=False),
            GD_LO: lambda change: CopyFileGDriveToLocalCommand(change, overwrite=False, delete_src_node_after=False),
            LO_different_LO: lambda change: _fail(change, LO_different_LO),  # TODO: support > 1 # of same tree type
            GD_different_GD: lambda change: _fail(change, GD_different_GD),  # TODO: support > 1 of same tree type
        },
        UserOpCode.CP_ONTO: {
            LO_same_LO: lambda change: CopyFileLocalToLocalCommand(change, overwrite=True),
            GD_same_GD: lambda change: CopyFileWithinGDriveCommand(change, overwrite=True),
            LO_GD: lambda change: CopyFileLocalToGDriveCommand(change, overwrite=True, delete_src_node_after=False),
            GD_LO: lambda change: CopyFileGDriveToLocalCommand(change, overwrite=True, delete_src_node_after=False),
            LO_different_LO: lambda change: _fail(change, LO_different_LO),  # TODO: support > 1 of same tree type
            GD_different_GD: lambda change: _fail(change, GD_different_GD),  # TODO: support > 1 of same tree type
        },

        UserOpCode.START_DIR_CP: {
            LO_same_LO: lambda change: StartCopyToLocalDirCommand(op=change),
            GD_same_GD: lambda change: StartCopyToGDriveFolderCommand(op=change),
            LO_GD: lambda change: StartCopyToGDriveFolderCommand(op=change),
            GD_LO: lambda change: StartCopyToLocalDirCommand(op=change),
            LO_different_LO: lambda change: StartCopyToLocalDirCommand(op=change),
            GD_different_GD: lambda change: StartCopyToGDriveFolderCommand(op=change),
        },
        UserOpCode.FINISH_DIR_CP: {
            LO_same_LO: lambda change: FinishCopyToLocalDirCommand(op=change, delete_src_node_after=False),
            GD_same_GD: lambda change: FinishCopyToGDriveFolderCommand(op=change, delete_src_node_after=False),
            LO_GD: lambda change: FinishCopyToGDriveFolderCommand(op=change, delete_src_node_after=False),
            GD_LO: lambda change: FinishCopyToLocalDirCommand(op=change, delete_src_node_after=False),
            LO_different_LO: lambda change: FinishCopyToLocalDirCommand(op=change, delete_src_node_after=False),
            GD_different_GD: lambda change: FinishCopyToGDriveFolderCommand(op=change, delete_src_node_after=False),
        },

        UserOpCode.MV: {
            LO_same_LO: lambda change: MoveFileLocalToLocalCommand(change, overwrite=False),
            GD_same_GD: lambda change: MoveFileWithinGDriveCommand(change, overwrite=False),
            LO_GD: lambda change: CopyFileLocalToGDriveCommand(change, overwrite=False, delete_src_node_after=True),
            GD_LO: lambda change: CopyFileGDriveToLocalCommand(change, overwrite=False, delete_src_node_after=True),
            LO_different_LO: lambda change: _fail(change, LO_different_LO),  # TODO: support > 1 of same tree type
            GD_different_GD: lambda change: _fail(change, GD_different_GD),  # TODO: support > 1 of same tree type
        },
        UserOpCode.MV_ONTO: {
            LO_same_LO: lambda change: MoveFileLocalToLocalCommand(change, overwrite=True),
            GD_same_GD: lambda change: MoveFileWithinGDriveCommand(change, overwrite=True),
            LO_GD: lambda change: CopyFileLocalToGDriveCommand(change, overwrite=True, delete_src_node_after=True),
            GD_LO: lambda change: CopyFileGDriveToLocalCommand(change, overwrite=True, delete_src_node_after=True),
            LO_different_LO: lambda change: _fail(change, LO_different_LO),  # TODO: support > 1 of same tree type
            GD_different_GD: lambda change: _fail(change, GD_different_GD),  # TODO: support > 1 of same tree type
        },

        UserOpCode.START_DIR_MV: {
            LO_same_LO: lambda change: StartCopyToLocalDirCommand(op=change),
            GD_same_GD: lambda change: StartCopyToGDriveFolderCommand(op=change),
            LO_GD: lambda change: StartCopyToGDriveFolderCommand(op=change),
            GD_LO: lambda change: StartCopyToLocalDirCommand(op=change),
            LO_different_LO: lambda change: StartCopyToLocalDirCommand(op=change),
            GD_different_GD: lambda change: StartCopyToGDriveFolderCommand(op=change),
        },
        UserOpCode.FINISH_DIR_MV: {
            LO_same_LO: lambda change: FinishCopyToLocalDirCommand(op=change, delete_src_node_after=True),
            GD_same_GD: lambda change: FinishCopyToGDriveFolderCommand(op=change, delete_src_node_after=True),
            LO_GD: lambda change: FinishCopyToGDriveFolderCommand(op=change, delete_src_node_after=True),
            GD_LO: lambda change: FinishCopyToLocalDirCommand(op=change, delete_src_node_after=True),
            LO_different_LO: lambda change: FinishCopyToLocalDirCommand(op=change, delete_src_node_after=True),
            GD_different_GD: lambda change: FinishCopyToGDriveFolderCommand(op=change, delete_src_node_after=True),
        },

        UserOpCode.CREATE_LINK: {
            # TODO link support
            LO_same_LO: lambda change: _fail(change, LO_same_LO),
            GD_same_GD: lambda change: _fail(change, GD_same_GD),
            LO_GD: lambda change: _fail(change, LO_GD),
            GD_LO: lambda change: _fail(change, GD_LO),
            LO_different_LO: lambda change: _fail(change, LO_different_LO),
            GD_different_GD: lambda change: _fail(change, GD_different_GD),
            GD: lambda change: _fail(change, GD),
            LO: lambda change: _fail(change, LO),
        },
    }