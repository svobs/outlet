import logging
from typing import Callable, Dict

from cmd.cmd_impl import CopyFileLocallyCommand, CreateGDriveFolderCommand, \
    CreatLocalDirCommand, DeleteGDriveFileCommand, DeleteLocalFileCommand, \
    DownloadFromGDriveCommand, \
    MoveFileGDriveCommand, \
    MoveFileLocallyCommand, \
    UploadToGDriveCommand
from cmd.cmd_interface import Command
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.change_action import ChangeAction, ChangeType

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

    def build_command(self, change_action: ChangeAction) -> Command:
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
