import os
import time
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import List

import file_util
import fmeta.content_hasher
from constants import FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT
from gdrive.client import GDriveClient
from index.uid_generator import UID
from model.display_node import DisplayNode
from model.fmeta import FMeta
from model.goog_node import FolderToAdd, GoogFile
from model.planning_node import FileDecoratorNode, FileToAdd, FileToMove, FileToUpdate


logger = logging.getLogger(__name__)


class CommandStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    STOPPED_ON_ERROR = 8
    COMPLETED_NO_OP = 9
    COMPLETED_OK = 10


class CommandContext:
    def __init__(self, staging_dir: str, config, tree_id, needs_gdrive: bool):
        self.staging_dir = staging_dir
        self.config = config
        if needs_gdrive:
            self.gdrive_client = GDriveClient(config=self.config, tree_id=tree_id)


class Command(ABC):
    def __init__(self, model_obj: DisplayNode = None):
        self._model = model_obj
        self._status = CommandStatus.NOT_STARTED
        self._error = None

    @abstractmethod
    def execute(self, context: CommandContext):
        pass

    @abstractmethod
    def get_total_work(self) -> int:
        """Return the total work needed to complete this task, as an integer for a progressbar widget"""
        return 0

    def needs_gdrive(self):
        return False

    def status(self) -> CommandStatus:
        return self._status

    def has_dependencies(self) -> bool:
        return self.get_dependencies() is not None

    def get_dependencies(self) -> List[int]:
        """Returns a list of UIDs"""
        return []

    def get_model(self) -> DisplayNode:
        return self._model

    def set_error(self, err):
        self._error = err
        self._status = CommandStatus.STOPPED_ON_ERROR


class CommandList:
    def __init__(self, uid: UID):
        self.uid: UID = uid
        self.create_ts = int(time.time())
        self._cmds: List[Command] = []

    def append(self, command: Command):
        self._cmds.append(command)

    def __iter__(self):
        return self._cmds.__iter__()

    def __len__(self):
        return self._cmds.__len__()

    def get_total_succeeded(self):
        total_succeeded = 0
        for command in self._cmds:
            if command.status() == CommandStatus.COMPLETED_OK:
                total_succeeded += 1
        return total_succeeded


class CopyFileLocallyCommand(Command):
    """Local-to-local add or update"""
    def __init__(self, model_obj: DisplayNode, overwrite: bool = False):
        super().__init__(model_obj)
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
            self._status = CommandStatus.COMPLETED_OK
        except file_util.IdenticalFileExistsError:
            # Not a real error. Note and proceed
            self._status = CommandStatus.COMPLETED_NO_OP
        except Exception as err:
            # Try to log helpful info
            logger.error(f'While copying file: src_path="{self._model.original_full_path}", dst_path="{self._model.dest_path}", : {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class DeleteLocalFileCommand(Command):
    def __init__(self, model_obj: DisplayNode, to_trash=True):
        super().__init__(model_obj)
        self.to_trash = to_trash

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, context: CommandContext):
        try:
            logger.debug(f'RM: tgt={self._model.full_path}')
            file_util.delete_file(self._model.full_path, self.to_trash)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.error(f'While deleting file: path={self._model.full_path}: {repr(err)}, to_trash={self.to_trash}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class MoveFileLocallyCommand(Command):
    def __init__(self, model_obj: DisplayNode):
        super().__init__(model_obj)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, FileToMove)
            logger.debug(f'MV: src={self._model.original_full_path}')
            logger.debug(f'    dst={self._model.dest_path}')
            file_util.move_file(self._model.original_full_path, self._model.dest_path)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.error(f'While moving file: dest_path="{self._model.dest_path}", '
                         f'orig_path="{self._model.original_full_path}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class UploadToGDriveCommand(Command):
    def __init__(self, model_obj: FileDecoratorNode, overwrite: bool = False):
        super().__init__(model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        return self._model.size_bytes

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        parent_id = '1f2oIc2KkCAOyYDisJdsxv081W8IzZ5go' # FIXME
        try:
            assert isinstance(self._model, FileDecoratorNode), f'For {self._model}'
            src_file_path = self._model.original_full_path
            name = self._model.src_node.name
            existing = context.gdrive_client.get_existing_files(parent_goog_id=parent_id, name=name)
            logger.debug(f'Found {len(existing.nodes)} existing files')
            if not self._overwrite:
                assert isinstance(self._model, FileToAdd)

                if len(existing.nodes) > 0:
                    self._error = f'While trying to add: found unexpected item(s) with the same name and parent: {existing.nodes}'
                    self._status = CommandStatus.STOPPED_ON_ERROR
                    return
                else:
                    context.gdrive_client.upload_new_file(src_file_path, parents=[parent_id])
                    self._status = CommandStatus.COMPLETED_OK
                    return
            else:
                assert isinstance(self._model, FileToUpdate)

                assert isinstance(self._model.src_node, FMeta), f'For {self._model.src_node}'
                assert isinstance(self._model.dst_node, GoogFile), f'For {self._model.src_node}'

                old_md5 = self._model.dst_node.md5
                old_size = self._model.dst_node.size_bytes
                new_md5 = self._model.src_node.md5
                new_size = self._model.src_node.size_bytes

                data_to_update = None
                if len(existing.nodes) > 0:
                    for existing_data, existing_node in zip(existing.raw_items, existing.nodes):
                        assert isinstance(existing_node, GoogFile)
                        if existing_node.md5 == old_md5 and existing.nodes[0].size_bytes != old_size:
                            data_to_update = existing_data
                        elif existing_node.md5 == new_md5 and existing_node.size_bytes == new_size:
                            logger.info(f'Identical already exists in Google Drive; will not update (md5={new_md5}, size={new_size})')
                            self._status = CommandStatus.COMPLETED_NO_OP
                            return
                if data_to_update:
                    context.gdrive_client.update_existing_file(data_to_update, src_file_path)
                    self._status = CommandStatus.COMPLETED_OK
                    return
                else:
                    self._error = f'While trying to update item in Google Drive: could not find item with matching meta!'
                    self._status = CommandStatus.STOPPED_ON_ERROR
                    return

        except Exception as err:
            logger.error(f'While uploading file to GDrive: dest_parent_id="{parent_id}", '
                         f'src_path="{self._model.original_full_path}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class DownloadFromGDriveCommand(Command):
    def __init__(self, model_obj: FileDecoratorNode, overwrite: bool = False):
        super().__init__(model_obj)
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
            staging_path = os.path.join(context.staging_dir, self._model.md5)
            file_exists = os.path.exists(dst_path)
            if file_exists and not self._overwrite:
                raise RuntimeError(f'Cannot "add" a file downloaded from Google Drive: dest file already exists: "{dst_path}"')
            elif not file_exists and self._overwrite:
                raise RuntimeError(f'Trying to update a local file which does not exist: {dst_path}')

            context.gdrive_client.download_file(file_id=src_goog_id, dest_path=staging_path)
            # verify contents:
            downloaded_md5 = fmeta.content_hasher.md5(staging_path)
            if downloaded_md5 != self._model.src_node.md5:
                raise RuntimeError(f'Downloaded MD5 ({downloaded_md5}) does not matched expected ({self._model.src_node.md5})!')

            # This will overwrite if the file already exists:
            file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path)
            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.error(f'While downloading file from GDrive: dest_path="{self._model.dest_path}", '
                         f'src_goog_id="{self._model.src_node.goog_id}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class CreateGDriveFolderCommand(Command):
    def __init__(self, model_obj: FolderToAdd, parent_goog_ids: List[str]):
        super().__init__(model_obj)
        self.parent_goog_ids = parent_goog_ids

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            new_folder_id = context.gdrive_client.create_folder(name=self._model.name, parents=self.parent_goog_ids)
            # TODO: update cache appropriately
        except Exception as err:
            logger.error(f'While creating folder on GDrive: name="{self._model.name}", '
                         f'parent_goog_ids="{self.parent_goog_ids}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class MoveFileGDriveCommand(Command):
    def __init__(self, model_obj: DisplayNode):
        super().__init__(model_obj)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        pass
        # TODO


class DeleteGDriveFileCommand(Command):
    def __init__(self, model_obj: DisplayNode, to_trash=True):
        super().__init__(model_obj)
        self.to_trash = to_trash

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        pass
        # TODO






