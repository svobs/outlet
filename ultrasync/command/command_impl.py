import logging
import os
import pathlib

import file_util
from command.change_action import ChangeAction, ChangeType
from command.command_interface import Command, CommandContext, CommandStatus, CopyNodeCommand, DeleteNodeCommand, TwoNodeCommand
from constants import EXPLICITLY_TRASHED, FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, NOT_TRASHED
from index.uid import UID
from model.display_node import DisplayNode
from model.local_disk_node import LocalDirNode, LocalFileNode
from model.goog_node import GoogFile, GoogNode

logger = logging.getLogger(__name__)


# LOCAL COMMANDS begin
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class CopyFileLocallyCommand(CopyNodeCommand):
    """Local-to-local add or update"""
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode, overwrite: bool = False):
        super().__init__(uid, change_action, tgt_node, src_node, overwrite)
        self.tag = f'{__class__.__name__}(uid={self.identifier}, tgt_uid={self.tgt_node.uid}, src_uid={self.src_node.uid}, ow={self.overwrite})'
        assert change_action.change_type == ChangeType.CP

    def get_total_work(self) -> int:
        return self.tgt_node.get_size_bytes()

    def execute(self, cxt: CommandContext):
        try:
            src_path = self.src_node.full_path
            dst_path = self.tgt_node.full_path
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(cxt.staging_dir, self.tgt_node.md5)
            logger.debug(f'CP: src={src_path}')
            logger.debug(f'    stg={staging_path}')
            logger.debug(f'    dst={dst_path}')
            if self.overwrite:
                file_util.copy_file_update(src_path=src_path, staging_path=staging_path,
                                           md5_expected=self.tgt_node.md5, dst_path=dst_path,
                                           md5_src=self.src_node.md5, verify=True)
            else:
                file_util.copy_file_new(src_path=src_path, staging_path=staging_path, dst_path=dst_path, md5_src=self.tgt_node.md5, verify=True)

            # update cache:
            local_node = cxt.cache_manager.build_local_file_node(full_path=dst_path)
            cxt.cache_manager.add_or_update_node(local_node)

            self._status = CommandStatus.COMPLETED_OK
        except file_util.IdenticalFileExistsError:
            # Not a real error. Nothing to do.
            # However make sure we still keep the cache manager in the loop - it's likely out of date:
            local_node = cxt.cache_manager.build_local_file_node(full_path=self.tgt_node.full_path)
            cxt.cache_manager.add_or_update_node(local_node)
            self._status = CommandStatus.COMPLETED_NO_OP
        except Exception as err:
            # Try to log helpful info
            logger.exception(f'While copying file: src_path="{self.src_node.full_path}", dst_path="{self.tgt_node.full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self.overwrite}, ' \
               f'status={self._status}, dst={self.tgt_node}'


class DeleteLocalFileCommand(DeleteNodeCommand):
    """
    Delete Local
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, to_trash=True, delete_empty_parent=False):
        super().__init__(uid, change_action, tgt_node, to_trash, delete_empty_parent)
        self.tag = f'{__class__.__name__}(uid={self.identifier}, tgt_uid={self.tgt_node.uid} to_trash={self.to_trash}, ' \
                   f'delete_empty_parent={self.delete_empty_parent})'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        try:
            deleted_nodes_list = []
            logger.debug(f'RM: tgt={self.tgt_node.full_path}')
            file_util.delete_file(self.tgt_node.full_path, self.to_trash)
            deleted_nodes_list.append(self.tgt_node)

            # TODO: reconsider deleting empty ancestors...
            if self.delete_empty_parent:
                parent_dir_path: str = str(pathlib.Path(self.tgt_node.full_path).parent)
                # keep going up the dir tree, deleting empty parents
                while os.path.isdir(parent_dir_path) and len(os.listdir(parent_dir_path)) == 0:
                    if self.to_trash:
                        logger.warning(f'MoveEmptyDirToTrash not implemented!')
                    else:
                        dir_node = cxt.cache_manager.get_node_for_local_path(parent_dir_path)
                        if dir_node:
                            deleted_nodes_list.append(dir_node)
                            os.rmdir(parent_dir_path)
                            logger.info(f'Removed empty dir: "{parent_dir_path}"')
                        else:
                            logger.error(f'Cannot remove directory because it could not be found in cache: {parent_dir_path}')
                            break
                    parent_dir_path = str(pathlib.Path(parent_dir_path).parent)

            logger.debug(f'Deleted {len(deleted_nodes_list)} nodes: notifying cacheman')
            for deleted_node in deleted_nodes_list:
                cxt.cache_manager.remove_node(deleted_node, self.to_trash)

            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While deleting file: path={self.tgt_node.full_path}: {repr(err)}, to_trash={self.to_trash}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'delete_empty_parent={self.delete_empty_parent}, status={self._status}, tgt_node={self.tgt_node}'


class MoveFileLocallyCommand(TwoNodeCommand):
    """
    Move/Rename Local -> Local
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode):
        super().__init__(uid, change_action, tgt_node, src_node)
        self.tag = f'{__class__.__name__}(uid={self.identifier}, act={change_action.change_type} tgt_uid={self.tgt_node.uid}, ' \
                   f'src_uid={self.src_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        try:
            logger.debug(f'MV: src={self.src_node.full_path}')
            logger.debug(f'    dst={self.tgt_node.full_path}')
            assert isinstance(self.src_node, LocalFileNode)
            assert isinstance(self.tgt_node, LocalFileNode)
            file_util.move_file(self.src_node.full_path, self.tgt_node.full_path)

            # Add to cache:
            local_node: LocalFileNode = cxt.cache_manager.build_local_file_node(full_path=self.tgt_node.full_path)
            cxt.cache_manager.add_or_update_node(local_node)
            if not os.path.exists(self.src_node.full_path):
                self.src_node.set_exists(False)
                cxt.cache_manager.remove_node(local_node)
            else:
                logger.warning(f'Src node still exists after move: {self.src_node.full_path}')
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While moving file: dest_path="{self.tgt_node.full_path}", orig_path="{self.src_node.full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, dst={self.tgt_node.full_path}'


class CreatLocalDirCommand(Command):
    """
    Create Local dir
    """

    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode):
        super().__init__(uid, change_action, tgt_node,)
        self.tag = f'{__class__.__name__}(uid={self.identifier}, act={change_action.change_type} tgt_uid={self.tgt_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        try:
            logger.debug(f'MKDIR: dst={self.tgt_node.full_path}')
            os.makedirs(name=self.tgt_node.full_path, exist_ok=True)

            # Add to cache:
            local_node = LocalDirNode(self.tgt_node.node_identifier, True)
            cxt.cache_manager.add_or_update_node(local_node)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While making local dir: dest_path="{self.tgt_node.full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, tgt_node={self.tgt_node}'

# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# LOCAL COMMANDS end


# GOOGLE DRIVE COMMANDS begin
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class UploadToGDriveCommand(CopyNodeCommand):
    """
    Copy Local -> GDrive
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode, overwrite: bool):
        super().__init__(uid, change_action, tgt_node, src_node, overwrite)
        self.tag = f'{__class__.__name__}(uid={self.identifier}, act={change_action.change_type} tgt_uid={self.tgt_node.uid} ' \
                   f'src_uid={self.src_node.uid} overwrite={overwrite}'

    def get_total_work(self) -> int:
        return self.tgt_node.get_size_bytes()

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        try:
            assert isinstance(self.tgt_node, GoogNode)
            # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
            src_file_path = self.src_node.full_path
            md5 = self.src_node.md5
            size_bytes = self.src_node.get_size_bytes()

            existing, existing_raw = cxt.gdrive_client.get_existing_file_with_same_parent_and_name_and_criteria(self.tgt_node, lambda x: True)
            if existing and existing.md5 == md5 and existing.get_size_bytes() == size_bytes:
                logger.info(f'Identical item already exists in Google Drive: (md5={md5}, size={size_bytes})')
                self._status = CommandStatus.COMPLETED_NO_OP
                # Update cache manager - it's likely out of date:
                cached_node = cxt.cache_manager.get_item_for_goog_id(goog_id=existing.goog_id)
                if cached_node:
                    if existing == cached_node:
                        logger.info(f'Identical already exists in Google Drive and local cache (UID={cached_node.uid})! Skipping.')
                        return
                    else:
                        logger.debug(f'Found existing node in cache for goog_id="{cached_node.goog_id}" but it needs update')
                else:
                    logger.debug(f'Item not found in cache; it will be inserted: goog_id="{existing.goog_id}"')

                # Target node will contain invalid UID anyway because it has no goog_id. Just remove it
                cxt.cache_manager.remove_node(self.tgt_node)
                cxt.cache_manager.add_or_update_node(existing)
                return

            if self.overwrite:
                if existing:
                    logger.info(f'Found existing item in Google Drive with same parent and name, but different content (overwrite={self.overwrite})')
                    goog_node: GoogNode = cxt.gdrive_client.update_existing_file(raw_item=existing_raw, local_full_path=src_file_path)
                else:
                    self._error = f'While trying to update item in Google Drive: could not find item with matching meta!'
                    self._status = CommandStatus.STOPPED_ON_ERROR
                    return
            else:  # not overwrite
                if existing:
                    self._error = f'While trying to add: found unexpected item(s) with the same name and parent: {existing}'
                    self._status = CommandStatus.STOPPED_ON_ERROR
                    return
                else:
                    parent_goog_id: str = cxt.gdrive_client.resolve_parent_ids_to_goog_ids(self.tgt_node)
                    goog_node: GoogNode = cxt.gdrive_client.upload_new_file(src_file_path, parent_goog_ids=parent_goog_id)

            cxt.cache_manager.remove_node(self.tgt_node)
            cxt.cache_manager.add_or_update_node(goog_node)
            self._status = CommandStatus.COMPLETED_OK
            return

        except Exception as err:
            logger.exception(f'While uploading file to GDrive: dest_parent_ids="{self.tgt_node.get_parent_uids()}", '
                             f'src_path="{self.src_node.full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self.overwrite}, ' \
               f'status={self._status}, model={self.tgt_node}'


class DownloadFromGDriveCommand(CopyNodeCommand):
    """
    Copy GDrive -> Local
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode, overwrite: bool):
        super().__init__(uid, change_action, tgt_node, src_node, overwrite)
        assert isinstance(self.src_node, GoogNode), f'For {self.src_node}'
        assert isinstance(self.tgt_node, LocalFileNode), f'For {self.tgt_node}'
        self.tag = f'{__class__.__name__}(uid={self.identifier}, act={change_action.change_type} tgt_uid={self.tgt_node.uid} ' \
                   f'src_uid={self.src_node.uid} overwrite={overwrite}'

    def get_total_work(self) -> int:
        return self.src_node.get_size_bytes()

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        try:
            assert isinstance(self.src_node, GoogFile), f'For {self.src_node}'
            src_goog_id = self.src_node.goog_id
            dst_path = self.tgt_node.full_path

            if os.path.exists(dst_path):
                item: LocalFileNode = cxt.cache_manager.build_local_file_node(full_path=dst_path)
                if item and item.md5 == self.src_node.md5:
                    logger.debug(f'Item already exists and appears valid: skipping download; will update cache and return ({dst_path})')
                    cxt.cache_manager.add_or_update_node(item)
                    self._status = CommandStatus.COMPLETED_NO_OP
                    return
                elif not self.overwrite:
                    raise RuntimeError(f'A different item already exists at the destination path: {dst_path}')
            elif self.overwrite:
                logger.warning(f'Doing an "update" for a local file which does not exist: {dst_path}')

            try:
                os.makedirs(name=cxt.staging_dir, exist_ok=True)
            except Exception:
                logger.error(f'Exception while making staging dir: {cxt.staging_dir}')
                raise
            staging_path = os.path.join(cxt.staging_dir, self.src_node.md5)

            if os.path.exists(staging_path):
                item: LocalFileNode = cxt.cache_manager.build_local_file_node(full_path=dst_path)
                if item and item.md5 == self.src_node.md5:
                    logger.debug(f'Found target item in staging dir; will move: ({staging_path} -> {dst_path})')
                    file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)
                    cxt.cache_manager.add_or_update_node(item)
                    self._status = CommandStatus.COMPLETED_OK
                    return
                else:
                    logger.debug(f'Found unknown file in the staging dir; removing: {staging_path}')
                    os.remove(staging_path)

            cxt.gdrive_client.download_file(file_id=src_goog_id, dest_path=staging_path)

            # verify contents:
            item: LocalFileNode = cxt.cache_manager.build_local_file_node(full_path=dst_path, staging_path=staging_path)
            if item.md5 != self.src_node.md5:
                raise RuntimeError(f'Downloaded MD5 ({item.md5}) does not matched expected ({self.src_node.md5})!')

            # This will overwrite if the file already exists:
            file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)

            cxt.cache_manager.add_or_update_node(item)

            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While downloading file from GDrive: dest_path="{self.tgt_node.full_path}", '
                             f'src_goog_id="{self.src_node.goog_id}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self.overwrite}, ' \
               f'status={self._status}, tgt_node={self.tgt_node}'


class CreateGDriveFolderCommand(Command):
    """
    Create GDrive FOLDER (sometimes a prerequisite to uploading a file)
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode):
        super().__init__(uid, change_action, tgt_node)
        assert change_action.change_type == ChangeType.MKDIR
        self.tag = f'{__class__.__name__}(uid={self.identifier}, act={change_action.change_type} tgt_uid={self.tgt_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        try:
            assert isinstance(self.tgt_node, GoogNode) and self.tgt_node.is_dir(), f'For {self.tgt_node}'

            parent_goog_id: str = cxt.gdrive_client.resolve_parent_ids_to_goog_ids(self.tgt_node)
            name = self.tgt_node.name
            existing = cxt.gdrive_client.get_existing_folder_with_parent_and_name(parent_goog_id=parent_goog_id, name=name)
            if len(existing.nodes) > 0:
                logger.info(f'Found {len(existing.nodes)} existing folders with parent={parent_goog_id} and name="{name}". '
                            f'Will use first found instead of creating a new folder.')
                goog_node: GoogNode = existing.nodes[0]
                goog_node.uid = self.tgt_node.uid
            else:
                goog_node = cxt.gdrive_client.create_folder(name=self.tgt_node.name, parent_goog_ids=[parent_goog_id], uid=self.tgt_node.uid)
                logger.info(f'Created GDrive folder successfully: uid={goog_node.uid} name="{goog_node.name}", goog_id="{goog_node.goog_id}"')
            assert goog_node.is_dir()
            assert goog_node.get_parent_uids(), f'Expected some parent_uids for: {goog_node}'
            # Add node to disk & in-memory caches:
            cxt.cache_manager.add_or_update_node(goog_node)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While creating folder on GDrive: name="{self.tgt_node.name}", '
                             f'parent_uids="{self.tgt_node.get_parent_uids()}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, tgt_node={self.tgt_node}'


class MoveFileGDriveCommand(TwoNodeCommand):
    """
    Move GDrive -> GDrive
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode):
        super().__init__(uid, change_action, tgt_node, src_node)
        assert change_action.change_type == ChangeType.MV
        self.tag = f'{__class__.__name__}(uid={self.identifier}, act={change_action.change_type} tgt_uid={self.tgt_node.uid}, ' \
                   f'src_uid={self.src_node.uid}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        try:
            assert isinstance(self.tgt_node, GoogFile), f'For {self.tgt_node}'
            assert isinstance(self.src_node, GoogFile), f'For {self.src_node}'
            # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
            src_parent_goog_id: str = cxt.gdrive_client.resolve_parent_ids_to_goog_ids(self.src_node)
            dst_parent_goog_id: str = cxt.gdrive_client.resolve_parent_ids_to_goog_ids(self.tgt_node)
            src_goog_id = self.src_node.goog_id
            assert not self.tgt_node.goog_id
            dst_name = self.tgt_node.name

            existing_src, raw = cxt.gdrive_client.get_existing_file_with_same_parent_and_name_and_criteria(self.src_node,
                                                                                                      lambda x: x.goog_id == src_goog_id)
            if existing_src:
                goog_node = cxt.gdrive_client.modify_meta(goog_id=src_goog_id, remove_parents=[src_parent_goog_id], add_parents=[dst_parent_goog_id],
                                                          name=dst_name)

                assert goog_node.name == self.tgt_node.name and goog_node.uid == self.src_node.uid

                # Update master cache. The tgt_node must be removed (it has a different UID). The src_node will be updated.
                cxt.cache_manager.remove_node(self.tgt_node)
                cxt.cache_manager.add_or_update_node(goog_node)
                self._status = CommandStatus.COMPLETED_OK
                return
            else:
                # did not find the src file; see if our operation was already completed
                existing_dst, raw = cxt.gdrive_client.get_existing_file_with_same_parent_and_name_and_criteria(self.tgt_node,
                                                                                                          lambda x: x.goog_id == src_goog_id)
                if existing_dst:
                    # Update cache manager as it's likely out of date:
                    cached_node = cxt.cache_manager.get_item_for_uid(uid=existing_dst.uid)
                    if cached_node and existing_dst == cached_node:
                        logger.info(f'Identical already exists in Google Drive and local cache (UID={existing_dst.uid})! Skipping.')
                        self._status = CommandStatus.COMPLETED_NO_OP
                        return

                    assert existing_dst.uid == self.src_node.uid and existing_dst.goog_id == self.src_node.goog_id,\
                        f'For {existing_dst} and {self.src_node}'
                    logger.info(f'Identical already exists in Google Drive; will update cache only (goog_id={existing_dst.goog_id})')
                    cxt.cache_manager.add_or_update_node(existing_dst)
                    cxt.cache_manager.remove_node(self.tgt_node)
                    self._status = CommandStatus.COMPLETED_NO_OP
                    return
                else:
                    raise RuntimeError(f'Could not find expected node in source or dest locations. Looks like the model is out of date '
                                       f'(goog_id={self.src_node.goog_id})')

        except Exception as err:
            logger.exception(f'While moving file within GDrive: dest_parent_ids="{self.tgt_node.get_parent_uids()}", '
                             f'src_node="{self.src_node}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, tgt-node={self.tgt_node}'


class DeleteGDriveFileCommand(DeleteNodeCommand):
    """
    Delete GDrive
    """
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, to_trash=True, delete_empty_parent=False):
        super().__init__(uid, change_action, tgt_node, to_trash, delete_empty_parent)
        self.tag = f'{__class__.__name__}(uid={self.identifier}, to_trash={self.to_trash}, delete_empty_parent={self.delete_empty_parent})'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        try:
            assert isinstance(self.tgt_node, GoogFile)
            tgt_goog_id = self.tgt_node.goog_id

            existing = cxt.gdrive_client.get_existing_file_with_same_parent_and_name_and_criteria(self.tgt_node, lambda x: x.goog_id == tgt_goog_id)
            if not existing:
                raise RuntimeError('Cannot delete: not found in GDrive!')

            if self.delete_empty_parent:
                # TODO
                logger.error('delete_empty_parent is not implemented!')

            if self.to_trash and existing.trashed != NOT_TRASHED:
                logger.info(f'Item is already trashed: {existing}')
                self._status = CommandStatus.COMPLETED_NO_OP
                return

            if self.to_trash:
                cxt.gdrive_client.trash(self.tgt_node.goog_id)
                self.tgt_node.trashed = EXPLICITLY_TRASHED
            else:
                cxt.gdrive_client.hard_delete(self.tgt_node.goog_id)

            cxt.cache_manager.remove_node(self.tgt_node, self.to_trash)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While deleting from GDrive (to_trash={self.to_trash}, node={self.tgt_node}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'status={self._status}, model={self.tgt_node}'


# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# GOOGLE DRIVE COMMANDS end

