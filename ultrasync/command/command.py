import os
import time
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import List

import file_util
from constants import FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from model.category import Category
from model.display_node import DisplayNode
from model.planning_node import FileToAdd, FileToMove, FileToUpdate


logger = logging.getLogger(__name__)


class CommandStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    STOPPED_ON_ERROR = 8
    COMPLETED_NO_OP = 9
    COMPLETED_OK = 10


class CommandContext:
    def __init__(self, staging_dir: str):
        self.staging_dir = staging_dir


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

    def status(self) -> CommandStatus:
        return self._status

    def has_dependencies(self) -> bool:
        return self.get_dependencies() is not None

    def get_dependencies(self) -> List[int]:
        """Returns a list of UIDs"""
        return []

    def get_model(self) -> DisplayNode:
        return self._model


class CommandList:
    def __init__(self, uid: int):
        self.uid: int = uid
        self.create_ts = int(time.time())
        self._cmds: List[Command] = []

    def append(self, command: Command):
        self._cmds.append(command)


class CopyFileLocallyCommand(Command):
    """Local-to-local add or update"""
    def __init__(self, model_obj: DisplayNode, overwrite: bool = False):
        super().__init__(model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        return self._model.size_bytes

    def execute(self, context: CommandContext):
        try:
            src_path = self._model.original_full_path
            dst_path = self._model.dest_path
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(context.staging_dir, self._model.md5)
            logger.debug(f'CP: src={src_path}')
            logger.debug(f'    stg={staging_path}')
            logger.debug(f'    dst={dst_path}')
            if self.overwrite:
                assert isinstance(self._model, FileToUpdate)
                file_util.copy_file_update(src_path=src_path, staging_path=staging_path, md5_expected=self._model.dst_node.md5, dst_path=dst_path, md5_src=self._model.src_node.md5, verify=True)
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
            logger.error(f'While moving file: root="{self._model.root_path}",'
                         f' dest_path="{self._model.dest_path}", orig_path="{self._model.original_full_path}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class UploadToGDriveCommand(Command):
    def __init__(self, model_obj: DisplayNode, overwrite: bool = False):
        super().__init__(model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        return self._model.size_bytes

    def execute(self, context: CommandContext):
        try:
            pass
        except Exception as err:
            logger.error(f'While uploading file from GDrive: root="{self._model.root_path}",'
                         f' dest_path="{self._model.dest_path}", orig_path="{self._model.original_full_path}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class DownloadFromGDriveCommand(Command):
    def __init__(self, model_obj: DisplayNode, overwrite: bool = False):
        super().__init__(model_obj)
        self._overwrite = overwrite

    def get_total_work(self) -> int:
        return self._model.size_bytes

    def execute(self, context: CommandContext):
        try:
            pass
        except Exception as err:
            logger.error(f'While downloading file from GDrive: root="{self._model.root_path}",'
                         f' dest_path="{self._model.dest_path}", orig_path="{self._model.original_full_path}": {repr(err)}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err


class MoveFileGDriveCommand(Command):
    def __init__(self, model_obj: DisplayNode):
        super().__init__(model_obj)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT



class DeleteGDriveFileCommand(Command):
    def __init__(self, model_obj: DisplayNode, to_trash=True):
        super().__init__(model_obj)
        self.to_trash = to_trash

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT






