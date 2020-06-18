import copy
import logging
import os
import pathlib
from typing import Optional

import file_util
from command.command_interface import Command, CommandContext, CommandStatus
from constants import EXPLICITLY_TRASHED, FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, NOT_TRASHED
from model.display_node import DirNode, DisplayNode
from model.fmeta import FMeta
from model.goog_node import FolderToAdd, GoogFile, GoogNode
from model.planning_node import FileDecoratorNode, FileToAdd, FileToMove, FileToUpdate, LocalDirToAdd

logger = logging.getLogger(__name__)


# LOCAL COMMANDS begin
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class CopyFileLocallyCommand(Command):
    """Local-to-local add or update"""
    def __init__(self, uid, model_obj: DisplayNode, overwrite: bool = False):
        super().__init__(uid, model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        return self._model.size_bytes

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, FileDecoratorNode)
            src_path = self._model.original_full_path
            dst_path = self._model.dest_path
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(context.staging_dir, self._model.md5)
            logger.debug(f'CP: src={src_path}')
            logger.debug(f'    stg={staging_path}')
            logger.debug(f'    dst={dst_path}')
            if self._overwrite:
                assert isinstance(self._model, FileToUpdate)
                assert isinstance(self._model.src_node, FMeta)
                assert isinstance(self._model.dst_node, FMeta)
                file_util.copy_file_update(src_path=src_path, staging_path=staging_path,
                                           md5_expected=self._model.dst_node.md5, dst_path=dst_path,
                                           md5_src=self._model.src_node.md5, verify=True)
            else:
                assert isinstance(self._model, FileToAdd)
                file_util.copy_file_new(src_path=src_path, staging_path=staging_path, dst_path=dst_path, md5_src=self._model.md5, verify=True)

            # update cache:
            local_node = context.cache_manager.build_fmeta(full_path=self._model.dest_path)
            context.cache_manager.add_or_update_node(local_node)

            self._status = CommandStatus.COMPLETED_OK
        except file_util.IdenticalFileExistsError:
            # Not a real error. Nothing to do.
            # However make sure we still keep the cache manager in the loop - it's likely out of date:
            local_node = context.cache_manager.build_fmeta(full_path=self._model.dest_path)
            context.cache_manager.add_or_update_node(local_node)
            self._status = CommandStatus.COMPLETED_NO_OP
        except Exception as err:
            # Try to log helpful info
            logger.exception(f'While copying file: src_path="{self._model.original_full_path}", dst_path="{self._model.dest_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self._overwrite}, ' \
               f'status={self._status}, model={self._model}'


class DeleteLocalFileCommand(Command):
    """
    Delete Local
    """
    def __init__(self, uid, model_obj: DisplayNode, to_trash=True, delete_empty_parent=False):
        super().__init__(uid, model_obj)
        self.to_trash = to_trash
        self.delete_empty_parent = delete_empty_parent

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, context: CommandContext):
        try:
            deleted_nodes_list = []
            logger.debug(f'RM: tgt={self._model.full_path}')
            file_util.delete_file(self._model.full_path, self.to_trash)
            deleted_nodes_list.append(self._model)

            if self.delete_empty_parent:
                parent_dir_path: str = str(pathlib.Path(self._model.full_path).parent)
                # keep going up the dir tree, deleting empty parents
                while os.path.isdir(parent_dir_path) and len(os.listdir(parent_dir_path)) == 0:
                    if self.to_trash:
                        logger.warning(f'MoveEmptyDirToTrash not implemented!')
                    else:
                        os.rmdir(parent_dir_path)
                        logger.info(f'Removed empty dir: "{parent_dir_path}"')
                        dir_node = context.cache_manager.get_node_for_local_path(parent_dir_path)
                        if dir_node:
                            deleted_nodes_list.append(dir_node)
                    parent_dir_path = str(pathlib.Path(parent_dir_path).parent)

            logger.debug(f'Deleted {len(deleted_nodes_list)} nodes: notifying cacheman')
            for deleted_node in deleted_nodes_list:
                context.cache_manager.remove_node(deleted_node, self.to_trash)

            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While deleting file: path={self._model.full_path}: {repr(err)}, to_trash={self.to_trash}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'delete_empty_parent={self.delete_empty_parent}, status={self._status}, model={self._model}'


class MoveFileLocallyCommand(Command):
    """
    Move/Rename Local -> Local
    """
    def __init__(self, uid, model_obj: DisplayNode):
        super().__init__(uid, model_obj)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, FileToMove)
            logger.debug(f'MV: src={self._model.original_full_path}')
            logger.debug(f'    dst={self._model.dest_path}')
            file_util.move_file(self._model.original_full_path, self._model.dest_path)

            # Add to cache:
            local_node = context.cache_manager.build_fmeta(full_path=self._model.dest_path)
            context.cache_manager.add_or_update_node(local_node)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While moving file: dest_path="{self._model.dest_path}", orig_path="{self._model.original_full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, model={self._model}'


class CreatLocalDirCommand(Command):
    """
    Create Local dir
    """

    def __init__(self, uid, model_obj: DisplayNode):
        super().__init__(uid, model_obj)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, LocalDirToAdd)
            logger.debug(f'MKDIR: dst={self._model.full_path}')
            os.makedirs(name=self._model.full_path, exist_ok=True)

            # Add to cache:
            local_node = DirNode(self._model.node_identifier)
            context.cache_manager.add_or_update_node(local_node)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While making local dir: dest_path="{self._model.full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, model={self._model}'

# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# LOCAL COMMANDS end


# GOOGLE DRIVE COMMANDS begin
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼


class UploadToGDriveCommand(Command):
    """
    Copy Local -> GDrive
    """
    def __init__(self, uid, model_obj: FileDecoratorNode, overwrite: bool = False):
        super().__init__(uid, model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        return self._model.size_bytes

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, FileDecoratorNode), f'For {self._model}'
            # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
            parent_goog_id: str = context.resolve_parent_ids_to_goog_ids(self._model)
            src_file_path = self._model.original_full_path
            name = self._model.src_node.name
            new_md5 = self._model.src_node.md5
            new_size = self._model.src_node.size_bytes

            existing = context.gdrive_client.get_existing_file_with_parent_and_name(parent_goog_id=parent_goog_id, name=name)
            logger.debug(f'Found {len(existing.nodes)} existing files with parent={parent_goog_id} and name={name}')
            if self._overwrite:
                assert isinstance(self._model, FileToUpdate)

                assert isinstance(self._model.src_node, FMeta), f'For {self._model.src_node}'
                assert isinstance(self._model.dst_node, GoogFile), f'For {self._model.src_node}'

                old_md5 = self._model.dst_node.md5
                old_size = self._model.dst_node.size_bytes

                data_to_update: Optional[GoogFile] = None
                if len(existing.nodes) > 0:
                    for existing_data, existing_node in zip(existing.raw_items, existing.nodes):
                        assert isinstance(existing_node, GoogFile)
                        if existing_node.md5 == old_md5 and existing.nodes[0].size_bytes != old_size:
                            data_to_update = existing_node
                        elif existing_node.md5 == new_md5 and existing_node.size_bytes == new_size:
                            logger.info(f'Identical item already exists in Google Drive: (md5={new_md5}, size={new_size})')
                            self._status = CommandStatus.COMPLETED_NO_OP
                            # Update cache manager - it's likely out of date:
                            cached_node = context.cache_manager.get_goog_node(parent_uid=self._model.parent_uids[0], goog_id=existing_node.goog_id)
                            if cached_node:
                                # kludge: make sure this field matches for equals func
                                existing_node.uid = cached_node.uid
                                if existing_node == cached_node:
                                    logger.info(f'Identical already exists in Google Drive and local cache (UID={existing_node.uid})! Skipping.')
                                    if cached_node.uid != self._model.uid:
                                        # Get rid of the planning node if there is one
                                        # (this code is getting hairy...)
                                        context.cache_manager.remove_node(self._model, False)
                                    return
                                else:
                                    logger.debug(f'Found existing node in cache for goog_id="{existing_node.goog_id}" but it needs update')
                            else:
                                logger.debug(f'Item not found in cache; it will be inserted: goog_id="{existing_node.goog_id}"')

                            existing_node.uid = self._model.uid
                            existing_node.parent_uids = self._model.parent_uids
                            context.cache_manager.add_or_update_node(existing_node)
                            return
                if data_to_update:
                    goog_node: GoogNode = context.gdrive_client.update_existing_file(raw_item=data_to_update, local_full_path=src_file_path,
                                                                                     uid=self._model.uid)
                    # Need to add these in (GDrive client cannot resolve them):
                    goog_node.parent_uids = self._model.parent_uids

                else:
                    self._error = f'While trying to update item in Google Drive: could not find item with matching meta!'
                    self._status = CommandStatus.STOPPED_ON_ERROR
                    return
            else:
                assert isinstance(self._model, FileToAdd)

                if len(existing.nodes) > 0:
                    for existing_data, existing_node in zip(existing.raw_items, existing.nodes):
                        assert isinstance(existing_node, GoogFile)
                        if existing_node.md5 == new_md5 and existing_node.size_bytes == new_size:
                            self._status = CommandStatus.COMPLETED_NO_OP
                            # Update cache manager - it's likely out of date:
                            cached_node = context.cache_manager.get_goog_node(parent_uid=self._model.parent_uids[0], goog_id=existing_node.goog_id)
                            if cached_node:
                                # kludge: make sure this field matches for equals func
                                existing_node.uid = cached_node.uid
                                if existing_node == cached_node:
                                    logger.info(f'Identical already exists in Google Drive and local cache (UID={existing_node.uid})! Skipping.')
                                    if cached_node.uid != self._model.uid:
                                        # Get rid of the planning node if there is one
                                        # (this code is getting hairy...)
                                        context.cache_manager.remove_node(self._model, False)
                                    return
                                # Fall through! Wheee!
                            logger.info(f'Identical item already exists in Google Drive; will update cache (md5={new_md5}, size={new_size})')
                            existing_node.uid = self._model.uid
                            existing_node.parent_uids = self._model.parent_uids
                            context.cache_manager.add_or_update_node(existing_node)
                            return

                    # if we got here, item is in the way which has an unexpected MD5
                    self._error = f'While trying to add: found unexpected item(s) with the same name and parent: {existing.nodes}'
                    self._status = CommandStatus.STOPPED_ON_ERROR
                    return
                else:
                    # Note that we will reuse the FileToAdd's UID
                    goog_node: GoogNode = context.gdrive_client.upload_new_file(src_file_path, parent_goog_ids=parent_goog_id, uid=self._model.uid)
                    # Need to add these in (GDrive client cannot resolve them):
                    goog_node.parent_uids = self._model.parent_uids

            # Add node to disk & in-memory caches:
            context.cache_manager.add_or_update_node(goog_node)
            self._status = CommandStatus.COMPLETED_OK

        except Exception as err:
            logger.exception(f'While uploading file to GDrive: dest_parent_ids="{self._model.parent_uids}", '
                             f'src_path="{self._model.original_full_path}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self._overwrite}, ' \
               f'status={self._status}, model={self._model}'


class DownloadFromGDriveCommand(Command):
    """
    Copy GDrive -> Local
    """
    def __init__(self, uid, model_obj: FileDecoratorNode, overwrite: bool = False):
        super().__init__(uid, model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        assert isinstance(self._model, FileDecoratorNode), f'For {self._model}'
        return self._model.src_node.size_bytes

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, FileDecoratorNode), f'For {self._model}'
            assert isinstance(self._model.src_node, GoogFile), f'For {self._model.src_node}'
            src_goog_id = self._model.src_node.goog_id
            dst_path = self._model.dest_path

            if os.path.exists(dst_path):
                item: FMeta = context.cache_manager.build_fmeta(full_path=dst_path)
                if item and item.md5 == self._model.src_node.md5:
                    logger.debug(f'Item already exists and appears valid: skipping download; will update cache and return ({dst_path})')
                    context.cache_manager.add_or_update_node(item)
                    self._status = CommandStatus.COMPLETED_NO_OP
                    return
                elif not self._overwrite:
                    raise RuntimeError(f'A different item already exists at the destination path: {dst_path}')
            elif self._overwrite:
                logger.warning(f'Doing an "update" for a local file which does not exist: {dst_path}')

            try:
                os.makedirs(name=context.staging_dir, exist_ok=True)
            except Exception:
                logger.error(f'Exception while making staging dir: {context.staging_dir}')
                raise
            staging_path = os.path.join(context.staging_dir, self._model.md5)

            if os.path.exists(staging_path):
                item: FMeta = context.cache_manager.build_fmeta(full_path=dst_path)
                if item and item.md5 == self._model.src_node.md5:
                    logger.debug(f'Found target item in staging dir; will move: ({staging_path} -> {dst_path})')
                    file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)
                    context.cache_manager.add_or_update_node(item)
                    self._status = CommandStatus.COMPLETED_OK
                    return
                else:
                    logger.debug(f'Found unknown file in the staging dir; removing: {staging_path}')
                    os.remove(staging_path)

            context.gdrive_client.download_file(file_id=src_goog_id, dest_path=staging_path)

            # verify contents:
            item: FMeta = context.cache_manager.build_fmeta(full_path=dst_path, staging_path=staging_path)
            if item.md5 != self._model.src_node.md5:
                raise RuntimeError(f'Downloaded MD5 ({item.md5}) does not matched expected ({self._model.src_node.md5})!')

            # This will overwrite if the file already exists:
            file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)

            context.cache_manager.add_or_update_node(item)

            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While downloading file from GDrive: dest_path="{self._model.dest_path}", '
                             f'src_goog_id="{self._model.src_node.goog_id}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, overwrite={self._overwrite}, ' \
               f'status={self._status}, model={self._model}'


class CreateGDriveFolderCommand(Command):
    """
    Create GDrive FOLDER (sometimes a prerequisite to uploading a file)
    """
    def __init__(self, uid, model_obj: FolderToAdd):
        super().__init__(uid, model_obj)
        assert isinstance(self._model, FolderToAdd) and model_obj.parent_uids, f'For {self._model}'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            parent_goog_id: str = context.resolve_parent_ids_to_goog_ids(self._model)
            name = self._model.name
            existing = context.gdrive_client.get_existing_folder_with_parent_and_name(parent_goog_id=parent_goog_id, name=name)
            if len(existing.nodes) > 0:
                logger.info(f'Found {len(existing.nodes)} existing folders with parent={parent_goog_id} and name="{name}". '
                            f'Will use first found instead of creating a new folder.')
                goog_node: GoogNode = existing.nodes[0]
                goog_node.uid = self._model.uid
            else:
                goog_node = context.gdrive_client.create_folder(name=self._model.name, parent_goog_ids=[parent_goog_id], uid=self._model.uid)
                logger.info(f'Created GDrive folder successfully: uid={goog_node.uid} name="{goog_node.name}", goog_id="{goog_node.goog_id}"')
            assert goog_node.is_dir()
            # Need to add these manually:
            goog_node.parent_uids = self._model.parent_uids
            assert goog_node.parent_uids, f'Expected some parent_uids for: {goog_node}'
            # Add node to disk & in-memory caches:
            context.cache_manager.add_or_update_node(goog_node)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While creating folder on GDrive: name="{self._model.name}", parent_uids="{self._model.parent_uids}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, model={self._model}'


class MoveFileGDriveCommand(Command):
    """
    Move GDrive -> GDrive
    """
    def __init__(self, uid, model_obj: DisplayNode):
        super().__init__(uid, model_obj)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, FileToMove), f'For {self._model}'
            assert isinstance(self._model.src_node, GoogFile)
            # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
            dst_parent_goog_id: str = context.resolve_parent_ids_to_goog_ids(self._model)
            src_parent_goog_id: str = context.resolve_parent_ids_to_goog_ids(self._model.src_node)
            src_name = self._model.src_node.name
            dst_name = self._model.name

            # Err...should have just done a single lookup by ID. Well, code's written. Prob fix later

            existing = context.gdrive_client.get_existing_file_with_parent_and_name(parent_goog_id=src_parent_goog_id, name=src_name)
            logger.debug(f'Found {len(existing.nodes)} matching src files with parent={src_parent_goog_id} and name={src_name}')

            src_node_found = None
            if len(existing.nodes) > 0:
                for existing_data, existing_node in zip(existing.raw_items, existing.nodes):
                    assert isinstance(existing_node, GoogFile)
                    if existing_node.goog_id == self._model.src_node.goog_id:
                        src_node_found = existing_node
                        break

            if src_node_found:
                context.gdrive_client.modify_meta(goog_id=self._model.src_node.goog_id, uid=self._model.uid,
                                                  remove_parents=[src_parent_goog_id],
                                                  add_parents=[dst_parent_goog_id], name=dst_name)

                goog_node = copy.copy(self._model.src_node)
                goog_node.name = self._model.name
                goog_node.parent_uids = self._model.parent_uids

                # TODO: see if cache already contains item at destination
                # Add node to disk & in-memory caches:
                context.cache_manager.add_or_update_node(goog_node)
                self._status = CommandStatus.COMPLETED_OK
                return
            else:
                # did not find the target file; see if our operation was already completed
                dest = context.gdrive_client.get_existing_file_with_parent_and_name(parent_goog_id=dst_parent_goog_id, name=dst_name)
                logger.debug(f'Found {len(dest.nodes)} matching dest files with parent={dst_parent_goog_id} and name={dst_name}')

                # FIXME: this code is duplicated 3 times. Consolidate
                dst_node_found: Optional[GoogFile] = None
                if len(existing.nodes) > 0:
                    for existing_data, existing_node in zip(existing.raw_items, existing.nodes):
                        assert isinstance(existing_node, GoogFile)
                        if existing_node.goog_id == self._model.src_node.goog_id:
                            dst_node_found = existing_node
                            break
                if dst_node_found:
                    # Update cache manager as it's likely out of date:
                    cached_node = context.cache_manager.get_goog_node(parent_uid=self._model.parent_uids[0], goog_id=dst_node_found.goog_id)
                    if cached_node:
                        # kludge: make sure this field matches for equals func
                        dst_node_found.uid = cached_node.uid
                        if dst_node_found == cached_node:
                            logger.info(f'Identical already exists in Google Drive and local cache (UID={dst_node_found.uid})! Skipping.')
                            if cached_node.uid != self._model.uid:
                                # Get rid of the planning node if there is one
                                context.cache_manager.remove_node(self._model, False)
                            return
                        # Fall through! Wheee!

                    logger.info(f'Identical already exists in Google Drive; will update cache only (goog_id={dst_node_found})')
                    dst_node_found.uid = self._model.uid
                    dst_node_found.parent_uids = self._model.parent_uids
                    context.cache_manager.add_or_update_node(dst_node_found)
                    self._status = CommandStatus.COMPLETED_NO_OP
                    return
                else:
                    raise RuntimeError(f'Could not find expected node in source or dest locations. Looks like the model is out of date '
                                       f'(goog_id={self._model.src_node.goog_id})')

        except Exception as err:
            logger.exception(f'While moving file within GDrive: dest_parent_ids="{self._model.parent_uids}", '
                             f'src_node="{self._model.src_node}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, model={self._model}'


class DeleteGDriveFileCommand(Command):
    """
    Delete GDrive
    """
    def __init__(self, uid, model_obj: DisplayNode, to_trash=True, delete_empty_parent=False):
        super().__init__(uid, model_obj)
        self.to_trash = to_trash
        self.delete_empty_parent = delete_empty_parent
        self.tag = f'{__class__.__name__}(uid={self.identifier}, to_trash={self.to_trash}, delete_empty_parent={self.delete_empty_parent})'

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, GoogFile)

            parent_goog_id: str = context.resolve_parent_ids_to_goog_ids(self._model)
            # did not find the target file; see if our operation was already completed
            existing = context.gdrive_client.get_existing_file_with_parent_and_name(parent_goog_id=parent_goog_id, name=self._model.name)
            logger.debug(f'Found {len(existing.nodes)} matching dest files with parent={parent_goog_id} and name={self._model.name}')

            target_node = None
            if len(existing.nodes) > 0:
                for existing_data, existing_node_x in zip(existing.raw_items, existing.nodes):
                    assert isinstance(existing_node_x, GoogFile)
                    if existing_node_x.goog_id == self._model.goog_id:
                        target_node = existing_node_x
                        break

            if not target_node:
                raise RuntimeError('Cannot delete: not found in GDrive!')

            if self.delete_empty_parent:
                # TODO
                logger.error('delete_empty_parent is not implemented!')

            if self.to_trash and target_node.trashed != NOT_TRASHED:
                logger.info(f'Item is already trashed: {target_node}')
                self._status = CommandStatus.COMPLETED_NO_OP
                return

            if self.to_trash:
                context.gdrive_client.trash(self._model.goog_id)
                self._model.trashed = EXPLICITLY_TRASHED
            else:
                context.gdrive_client.hard_delete(self._model.goog_id)

            context.cache_manager.remove_node(self._model, self.to_trash)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While deleting from GDrive (to_trash={self.to_trash}, node={self._model}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'status={self._status}, model={self._model}'


# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# GOOGLE DRIVE COMMANDS end

