import logging
from typing import Callable, Dict

from backend.executor.command.cmd_impl import CopyFileGDriveCommand, CopyFileLocallyCommand, CreateGDriveFolderCommand, \
    CreatLocalDirCommand, DeleteGDriveNodeCommand, DeleteLocalFileCommand, \
    DownloadFromGDriveCommand, \
    MoveFileGDriveCommand, \
    MoveFileLocallyCommand, \
    UploadToGDriveCommand
from backend.executor.command.cmd_interface import Command
from constants import TreeType
from model.user_op import UserOp, UserOpType

logger = logging.getLogger(__name__)


def _make_key(tree_type_src, tree_type_dst=None):
    if tree_type_dst:
        return f'{tree_type_src}->{tree_type_dst}'
    return f'{tree_type_src}'


LO = _make_key(TreeType.LOCAL_DISK)
GD = _make_key(TreeType.GDRIVE)
LO_LO = _make_key(TreeType.LOCAL_DISK, TreeType.LOCAL_DISK)
GD_GD = _make_key(TreeType.GDRIVE, TreeType.GDRIVE)
LO_GD = _make_key(TreeType.LOCAL_DISK, TreeType.GDRIVE)
GD_LO = _make_key(TreeType.GDRIVE, TreeType.LOCAL_DISK)


class CommandBuilder:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CommandBuilder
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid_generator):
        self._uid_generator = uid_generator
        self._build_dict: Dict[UserOpType, Dict[str, Callable]] = _populate_build_dict()

    def build_command(self, op: UserOp) -> Command:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Building command for UserOp={op}')

        # TODO: [improvement] look up MD5 for src_node and use a closer node

        if op.dst_node:
            # Src AND Dst:
            tree_type_key = _make_key(op.src_node.node_identifier.tree_type, op.dst_node.node_identifier.tree_type)
        else:
            # Only Src:
            tree_type_key = _make_key(op.src_node.node_identifier.tree_type)

        tree_type_dict = self._build_dict.get(op.op_type)
        if not tree_type_dict:
            raise RuntimeError(f'Unrecognized UserOpType: {op.op_type}')

        build_func = tree_type_dict.get(tree_type_key, None)
        if not build_func:
            raise RuntimeError(f'Bad tree type(s): {tree_type_key}, for UserOpType "{op.op_type.name}"')
        uid = self._uid_generator.next_uid()
        return build_func(uid, op)


def _populate_build_dict():
    """Every command has an associated target node and a UserOp. Many commands also have an associated source node."""
    return {UserOpType.MKDIR: {
        GD: lambda uid, change: CreateGDriveFolderCommand(op=change, uid=uid),
        LO: lambda uid, change: CreatLocalDirCommand(op=change, uid=uid)
    }, UserOpType.CP: {
        LO_LO: lambda uid, change: CopyFileLocallyCommand(uid, change, overwrite=False),

        # GD_GD: lambda uid, change: CopyFileGDriveCommand(uid, change),  # TODO

        LO_GD: lambda uid, change: UploadToGDriveCommand(uid, change, overwrite=False),

        GD_LO: lambda uid, change: DownloadFromGDriveCommand(uid, change, overwrite=False)
    }, UserOpType.MV: {
        LO_LO: lambda uid, change: MoveFileLocallyCommand(uid, change),

        GD_GD: lambda uid, change: MoveFileGDriveCommand(uid, change),

        LO_GD: lambda uid, change: UploadToGDriveCommand(uid, change, overwrite=False),

        GD_LO: lambda uid, change: DownloadFromGDriveCommand(uid, change, overwrite=False)
    }, UserOpType.RM: {
        # TODO: add support for trash
        LO: lambda uid, change: DeleteLocalFileCommand(uid, change, to_trash=False, delete_empty_parent=False),

        GD: lambda uid, change: DeleteGDriveNodeCommand(uid, change, to_trash=False, delete_empty_parent=False)
    }, UserOpType.UP: {
        LO_LO: lambda uid, change: CopyFileLocallyCommand(uid, change, overwrite=True),

        LO_GD: lambda uid, change: UploadToGDriveCommand(uid, change, overwrite=True),

        GD_LO: lambda uid, change: DownloadFromGDriveCommand(uid, change, overwrite=True),
    }}
