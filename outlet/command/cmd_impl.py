import logging
import os
import pathlib
from model.node_identifier import LocalFsIdentifier

from util import file_util
from model.op import Op, OpType
from command.cmd_interface import Command, CommandContext, CommandResult, CommandStatus, CopyNodeCommand, DeleteNodeCommand, TwoNodeCommand
from constants import FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, TrashStatus
from model.uid import UID
from model.node.local_disk_node import LocalDirNode, LocalFileNode
from model.node.gdrive_node import GDriveFile, GDriveNode

logger = logging.getLogger(__name__)


# LOCAL COMMANDS begin
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class CopyFileLocallyCommand(CopyNodeCommand):
    """Local-to-local add or update"""

    def __init__(self, uid: UID, op: Op, overwrite: bool = False):
        super().__init__(uid, op, overwrite)
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, ow={self.overwrite} ' \
                   f'src="{self.op.src_node.full_path}", dst="{self.op.dst_node.full_path}")'
        assert op.op_type == OpType.CP

    def get_total_work(self) -> int:
        return self.op.src_node.get_size_bytes()

    def execute(self, cxt: CommandContext) -> CommandResult:
        src_path = self.op.src_node.full_path
        dst_path = self.op.dst_node.full_path
        if not self.op.src_node.md5:
            # This can happen if the node was just added but lazy sig scan hasn't gotten to it yet. Just compute it ourselves here
            assert isinstance(self.op.src_node, LocalFileNode)
            self.op.src_node.md5 = store.local.content_hasher.compute_md5(src_path)
        md5 = self.op.src_node.md5
        # TODO: what if staging dir is not on same file system?
        staging_path = os.path.join(cxt.staging_dir, md5)
        logger.debug(f'CP: src={src_path}')
        logger.debug(f'    stg={staging_path}')
        logger.debug(f'    dst={dst_path}')
        if self.overwrite:
            file_util.copy_file_update(src_path=src_path, staging_path=staging_path,
                                       md5_expected=md5, dst_path=dst_path,
                                       md5_src=md5, verify=True)
        else:
            try:
                file_util.copy_file_new(src_path=src_path, staging_path=staging_path, dst_path=dst_path,
                                        md5_src=md5, verify=True)
            except file_util.IdenticalFileExistsError:
                # Not a real error. Nothing to do.
                # However make sure we still keep the cache manager in the loop - it's likely out of date. Calculate fresh stats:
                local_node = cxt.cacheman.build_local_file_node(full_path=dst_path)
                return CommandResult(CommandStatus.COMPLETED_NO_OP, to_upsert=[self.op.src_node, local_node])

        # update cache:
        local_node = cxt.cacheman.build_local_file_node(full_path=dst_path)
        assert local_node.uid == self.op.dst_node.uid, f'LocalNode={local_node}, DstNode={self.op.dst_node}'
        return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[self.op.src_node, local_node])

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self.overwrite}, ' \
               f'status={self.status()}, dst={self.op.dst_node}'


class DeleteLocalFileCommand(DeleteNodeCommand):
    """
    Delete Local. This supports deleting either a single file or an empty dir.
    """

    def __init__(self, uid: UID, op: Op, to_trash=True, delete_empty_parent=False):
        super().__init__(uid, op, to_trash, delete_empty_parent)
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, tgt_uid={self.op.src_node.uid} ' \
                   f'to_trash={self.to_trash}, delete_empty_parent={self.delete_empty_parent})'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        deleted_nodes_list = []
        logger.debug(f'RM: tgt={self.op.src_node.full_path}')

        if self.op.src_node.is_file():
            file_util.delete_file(self.op.src_node.full_path, self.to_trash)
        elif self.op.src_node.is_dir():
            file_util.delete_empty_dir(self.op.src_node.full_path, self.to_trash)
        else:
            raise RuntimeError(f'Not a file or dir: {self.op.src_node}')
        deleted_nodes_list.append(self.op.src_node)

        # TODO: reconsider deleting empty ancestors...
        if self.delete_empty_parent:
            parent_dir_path: str = str(pathlib.Path(self.op.src_node.full_path).parent)
            # keep going up the dir tree, deleting empty parents
            while os.path.isdir(parent_dir_path) and len(os.listdir(parent_dir_path)) == 0:
                if self.to_trash:
                    logger.warning(f'MoveEmptyDirToTrash not implemented!')
                else:
                    dir_node = cxt.cacheman.get_node_for_local_path(parent_dir_path)
                    if dir_node:
                        deleted_nodes_list.append(dir_node)
                        os.rmdir(parent_dir_path)
                        logger.info(f'Removed empty dir: "{parent_dir_path}"')
                    else:
                        logger.error(f'Cannot remove directory because it could not be found in cache: {parent_dir_path}')
                        break
                parent_dir_path = str(pathlib.Path(parent_dir_path).parent)

        return CommandResult(CommandStatus.COMPLETED_OK, to_delete=deleted_nodes_list)

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'delete_empty_parent={self.delete_empty_parent}, status={self.status()}, tgt_node={self.op.src_node}'


class MoveFileLocallyCommand(TwoNodeCommand):
    """
    Move/Rename Local -> Local
    """

    def __init__(self, uid: UID, op: Op):
        super().__init__(uid, op)
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, op={op.op_type.name} tgt_uid={self.op.dst_node.uid}, ' \
                   f'src_uid={self.op.src_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        logger.debug(f'MV: src={self.op.src_node.full_path}')
        logger.debug(f'    dst={self.op.dst_node.full_path}')
        assert isinstance(self.op.src_node, LocalFileNode)
        assert isinstance(self.op.dst_node, LocalFileNode)
        file_util.move_file(self.op.src_node.full_path, self.op.dst_node.full_path)

        # Add to cache:
        local_node: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=self.op.dst_node.full_path)
        to_upsert = [self.op.src_node, local_node]
        to_delete = []
        if not os.path.exists(self.op.src_node.full_path):
            to_delete = [self.op.src_node]
            cxt.cacheman.remove_node(local_node)
        else:
            logger.warning(f'Src node still exists after move: {self.op.src_node.full_path}')
        return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=to_upsert, to_delete=to_delete)

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self.status()}, dst={self.op.dst_node.full_path}'


class CreatLocalDirCommand(Command):
    """
    Create Local dir
    """

    def __init__(self, uid: UID, op: Op):
        super().__init__(uid, op)
        assert op.op_type == OpType.MKDIR
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        logger.debug(f'MKDIR: dst={self.op.src_node.full_path}')
        os.makedirs(name=self.op.src_node.full_path, exist_ok=True)

        # Add to cache:
        assert isinstance(self.op.src_node.node_identifier, LocalFsIdentifier)
        local_node = LocalDirNode(self.op.src_node.node_identifier, True)
        return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[local_node])

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, status={self.status()}'


# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# LOCAL COMMANDS end


# GOOGLE DRIVE COMMANDS begin
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class UploadToGDriveCommand(CopyNodeCommand):
    """
    Copy Local -> GDrive
    """

    def __init__(self, uid: UID, op: Op, overwrite: bool):
        super().__init__(uid, op, overwrite)
        assert isinstance(self.op.dst_node, GDriveNode)
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, op={op.op_type.name} src_uid={self.op.src_node.uid} ' \
                   f'dst_uid={self.op.dst_node.uid} dst_parent_ids="{self.op.dst_node.get_parent_uids()} overwrite={overwrite}'

    def get_total_work(self) -> int:
        return self.op.src_node.get_size_bytes()

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.dst_node, GDriveNode)
        # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
        src_file_path = self.op.src_node.full_path
        md5 = self.op.src_node.md5
        size_bytes = self.op.src_node.get_size_bytes()

        existing, existing_raw = cxt.gdrive_client.get_single_file_with_parent_and_name_and_criteria(self.op.dst_node)
        if existing and existing.md5 == md5 and existing.get_size_bytes() == size_bytes:
            logger.info(f'Identical node already exists in Google Drive: (md5={md5}, size={size_bytes})')
            # Target node will contain invalid UID anyway because it has no goog_id. Just remove it
            return CommandResult(CommandStatus.COMPLETED_NO_OP, to_upsert=[self.op.src_node, existing], to_delete=[self.op.dst_node])

        if self.overwrite:
            if existing:
                logger.info(f'Found existing node in Google Drive with same parent and name, but different content (overwrite={self.overwrite})')
                goog_node: GDriveNode = cxt.gdrive_client.update_existing_file(name=existing.name, mime_type=existing_raw['mimeType'],
                                                                               goog_id=existing.goog_id, local_full_path=src_file_path)
            else:
                # be cautious and halt
                return self.set_error_result(f'While trying to update node in Google Drive: could not find node with matching meta!')

        else:  # not overwrite
            if existing:
                return self.set_error_result(f'While trying to add: found unexpected node(s) with the same name and parent: {existing}')
            else:
                parent_goog_id: str = cxt.cacheman.get_goog_id_for_parent(self.op.dst_node)
                goog_node: GDriveNode = cxt.gdrive_client.upload_new_file(src_file_path, parent_goog_ids=parent_goog_id, uid=self.op.dst_node.uid)

        return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[self.op.src_node, goog_node])

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self.overwrite}, ' \
               f'status={self.status()}, src={self.op.src_node} dst={self.op.dst_node}'


class DownloadFromGDriveCommand(CopyNodeCommand):
    """
    Copy GDrive -> Local
    """

    def __init__(self, uid: UID, op: Op, overwrite: bool):
        super().__init__(uid, op, overwrite)
        assert isinstance(self.op.src_node, GDriveNode), f'For {self.op.src_node}'
        assert isinstance(self.op.dst_node, LocalFileNode), f'For {self.op.dst_node}'
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, op={op.op_type.name} tgt_uid={self.op.dst_node.uid} ' \
                   f'src_uid={self.op.src_node.uid} overwrite={overwrite}'

    def get_total_work(self) -> int:
        return self.op.src_node.get_size_bytes()

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveFile) and self.op.src_node.md5, f'Bad src node: {self.op.src_node}'
        src_goog_id = self.op.src_node.goog_id
        dst_path = self.op.dst_node.full_path

        if os.path.exists(dst_path):
            node: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=dst_path, must_scan_signature=True)
            if node and node.md5 == self.op.src_node.md5:
                logger.debug(f'Item already exists and appears valid: skipping download; will update cache and return ({dst_path})')
                return CommandResult(CommandStatus.COMPLETED_NO_OP, to_upsert=[self.op.src_node, node])
            elif not self.overwrite:
                raise RuntimeError(f'A different node already exists at the destination path: {dst_path}')
        elif self.overwrite:
            logger.warning(f'Doing an "update" for a local file which does not exist: {dst_path}')

        try:
            os.makedirs(name=cxt.staging_dir, exist_ok=True)
        except Exception:
            logger.error(f'Exception while making staging dir: {cxt.staging_dir}')
            raise
        staging_path = os.path.join(cxt.staging_dir, self.op.src_node.md5)

        if os.path.exists(staging_path):
            node: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=dst_path)
            if node and node.md5 == self.op.src_node.md5:
                logger.debug(f'Found target node in staging dir; will move: ({staging_path} -> {dst_path})')
                file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)
                return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[self.op.src_node, node])
            else:
                logger.debug(f'Found unknown file in the staging dir; removing: {staging_path}')
                os.remove(staging_path)

        cxt.gdrive_client.download_file(file_id=src_goog_id, dest_path=staging_path)

        # verify contents:
        node: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=dst_path, staging_path=staging_path, must_scan_signature=True)
        if node.md5 != self.op.src_node.md5:
            raise RuntimeError(f'Downloaded MD5 ({node.md5}) does not matched expected ({self.op.src_node.md5})!')

        # This will overwrite if the file already exists:
        file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)

        return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[self.op.src_node, node])

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self.overwrite}, ' \
               f'status={self.status()}, dst_node={self.op.dst_node}'


class CreateGDriveFolderCommand(Command):
    """
    Create GDrive FOLDER (sometimes a prerequisite to uploading a file)
    """

    def __init__(self, uid: UID, op: Op):
        super().__init__(uid, op)
        assert op.op_type == OpType.MKDIR
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, src_node_uid={self.op.src_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveNode) and self.op.src_node.is_dir(), f'For {self.op.src_node}'

        parent_goog_id: str = cxt.cacheman.get_goog_id_for_parent(self.op.src_node)
        name = self.op.src_node.name
        existing = cxt.gdrive_client.get_folders_with_parent_and_name(parent_goog_id=parent_goog_id, name=name)
        if len(existing.nodes) > 0:
            logger.info(f'Found {len(existing.nodes)} existing folders with parent={parent_goog_id} and name="{name}". '
                        f'Will use first found instead of creating a new folder.')
            goog_node: GDriveNode = existing.nodes[0]
            goog_node.uid = self.op.src_node.uid
        else:
            goog_node = cxt.gdrive_client.create_folder(name=self.op.src_node.name, parent_goog_ids=[parent_goog_id], uid=self.op.src_node.uid)
            logger.info(f'Created GDrive folder successfully: uid={goog_node.uid} name="{goog_node.name}", goog_id="{goog_node.goog_id}"')

        assert goog_node.is_dir()
        assert goog_node.get_parent_uids(), f'Expected some parent_uids for: {goog_node}'
        return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[goog_node])

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self.status()}, src_node={self.op.src_node}'


class MoveFileGDriveCommand(TwoNodeCommand):
    """
    Move GDrive -> GDrive
    """

    def __init__(self, uid: UID, op: Op):
        super().__init__(uid, op)
        assert op.op_type == OpType.MV
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, dst_uid={self.op.dst_node.uid}, ' \
                   f'src_uid={self.op.src_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.dst_node, GDriveFile), f'For {self.op.dst_node}'
        assert isinstance(self.op.src_node, GDriveFile), f'For {self.op.src_node}'
        # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
        src_parent_goog_id: str = cxt.cacheman.get_goog_id_for_parent(self.op.src_node)
        dst_parent_goog_id: str = cxt.cacheman.get_goog_id_for_parent(self.op.dst_node)
        src_goog_id = self.op.src_node.goog_id
        assert not self.op.dst_node.goog_id
        dst_name = self.op.dst_node.name

        existing_src, raw = cxt.gdrive_client.get_single_file_with_parent_and_name_and_criteria(self.op.src_node,
                                                                                                lambda x: x.goog_id == src_goog_id)
        if existing_src:
            goog_node = cxt.gdrive_client.modify_meta(goog_id=src_goog_id, remove_parents=[src_parent_goog_id], add_parents=[dst_parent_goog_id],
                                                      name=dst_name)

            assert goog_node.name == self.op.dst_node.name and goog_node.uid == self.op.src_node.uid

            # Update master cache. The tgt_node must be removed (it has a different UID). The src_node will be updated.
            return CommandResult(CommandStatus.COMPLETED_OK, to_upsert=[self.op.src_node, goog_node], to_delete=[self.op.dst_node])
        else:
            # did not find the src file; see if our operation was already completed
            existing_dst, raw = cxt.gdrive_client.get_single_file_with_parent_and_name_and_criteria(self.op.dst_node,
                                                                                                    lambda x: x.goog_id == src_goog_id)
            if existing_dst:
                # Update cache manager as it's likely out of date:
                assert existing_dst.uid == self.op.src_node.uid and existing_dst.goog_id == self.op.src_node.goog_id, \
                    f'For {existing_dst} and {self.op.src_node}'
                logger.info(f'Identical already exists in Google Drive; will update cache only (goog_id={existing_dst.goog_id})')
                return CommandResult(CommandStatus.COMPLETED_NO_OP, to_upsert=[self.op.src_node, existing_dst], to_delete=[self.op.dst_node])
            else:
                raise RuntimeError(f'Could not find expected node in source or dest locations. Looks like the model is out of date '
                                   f'(goog_id={self.op.src_node.goog_id})')

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self.status()}, dst_node={self.op.dst_node}'


class DeleteGDriveNodeCommand(DeleteNodeCommand):
    """
    Delete GDrive
    """

    def __init__(self, uid: UID, op: Op, to_trash=True, delete_empty_parent=False):
        super().__init__(uid, op, to_trash, delete_empty_parent)
        self.tag = f'{__class__.__name__}(cmd_uid={self.identifier}, op_uid={op.op_uid}, to_trash={self.to_trash}, ' \
                   f'delete_empty_parent={self.delete_empty_parent})'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveNode)
        tgt_goog_id = self.op.src_node.goog_id

        existing = cxt.gdrive_client.get_single_node_with_parent_and_name_and_criteria(self.op.src_node, lambda x: x.goog_id == tgt_goog_id)
        if not existing:
            return CommandResult(CommandStatus.COMPLETED_NO_OP, to_delete=[self.op.src_node])

        if self.delete_empty_parent:
            # TODO
            logger.error('delete_empty_parent is not implemented!')

        if self.to_trash and existing.trashed != TrashStatus.NOT_TRASHED:
            logger.info(f'Item is already trashed: {existing}')
            return CommandResult(CommandStatus.COMPLETED_NO_OP, to_delete=[existing])

        if self.to_trash:
            cxt.gdrive_client.trash(self.op.src_node.goog_id)
            self.op.src_node.trashed = TrashStatus.EXPLICITLY_TRASHED
        else:
            cxt.gdrive_client.hard_delete(self.op.src_node.goog_id)

        return CommandResult(CommandStatus.COMPLETED_OK, to_delete=[self.op.src_node])

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'status={self.status()}, model={self.op.src_node}'

# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# GOOGLE DRIVE COMMANDS end
