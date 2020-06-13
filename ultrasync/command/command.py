import collections
import copy
import os
import time
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Deque, List, Optional

import treelib

import file_util
import fmeta.content_hasher
from constants import EXPLICITLY_TRASHED, FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, NOT_TRASHED
from gdrive.client import GDriveClient
from index.uid_generator import UID
from model.display_node import DirNode, DisplayNode
from model.fmeta import FMeta
from model.gdrive_whole_tree import GDriveWholeTree
from model.goog_node import FolderToAdd, GoogFile, GoogFolder, GoogNode
from model.planning_node import FileDecoratorNode, FileToAdd, FileToMove, FileToUpdate, LocalDirToAdd

logger = logging.getLogger(__name__)

"""
# ENUM CommandStatus
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼"""


class CommandStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    STOPPED_ON_ERROR = 8
    COMPLETED_NO_OP = 9
    COMPLETED_OK = 10


"""
# CLASS CommandContext
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼"""


class CommandContext:
    def __init__(self, staging_dir: str, application, tree_id: str, needs_gdrive: bool):
        self.staging_dir = staging_dir
        self.config = application.config
        self.cache_manager = application.cache_manager
        self.uid_generator = application.uid_generator
        if needs_gdrive:
            self.gdrive_client = GDriveClient(config=self.config, tree_id=tree_id)
            self.gdrive_tree: GDriveWholeTree = self.cache_manager.get_gdrive_whole_tree(tree_id=tree_id)

    def resolve_parent_ids_to_goog_ids(self, node: DisplayNode) -> str:
        parent_uids: List[UID] = node.parent_uids
        if not parent_uids:
            raise RuntimeError(f'Parents are required but item has no parents: {node}')

        # This will raise an exception if it cannot resolve:
        parent_goog_ids: List[str] = self.gdrive_tree.resolve_uids_to_goog_ids(parent_uids)

        if len(parent_goog_ids) == 0:
            raise RuntimeError(f'No parent Google IDs for: {node}')
        if len(parent_goog_ids) > 1:
            # not supported at this time
            raise RuntimeError(f'Too many parent Google IDs for: {node}')

        parent_goog_id: str = parent_goog_ids[0]
        return parent_goog_id


""" 
CLASS Command
▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼"""


class Command(treelib.Node, ABC):
    def __init__(self, uid, model_obj: DisplayNode = None):
        treelib.Node.__init__(self, identifier=uid)
        self._model = model_obj
        self._status = CommandStatus.NOT_STARTED
        self._error = None
        self.tag = f'{__class__.__name__}(uid={self.identifier})'

    @abstractmethod
    def execute(self, context: CommandContext):
        pass

    @abstractmethod
    def get_total_work(self) -> int:
        """Return the total work needed to complete this task, as an integer for a progressbar widget"""
        return 0

    def needs_gdrive(self):
        return False

    def completed_without_error(self):
        return self._status == CommandStatus.COMPLETED_OK or self._status == CommandStatus.COMPLETED_NO_OP

    def status(self) -> CommandStatus:
        return self._status

    def get_model(self) -> DisplayNode:
        return self._model

    def set_error(self, err):
        self._error = err
        self._status = CommandStatus.STOPPED_ON_ERROR

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, status={self._status}, model={self._model}'


""" 
CLASS CommandPlan
▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼"""


class CommandPlan:
    def __init__(self, uid: UID, cmd_tree):
        self.uid: UID = uid
        self.create_ts = int(time.time())
        self.tree: treelib.Tree = cmd_tree

    def get_breadth_first_list(self):
        """Returns the command tree as a list, in breadth-first order"""
        blist: List[Command] = []

        queue: Deque[Command] = collections.deque()
        # skip root:
        for child in self.tree.children(self.tree.root):
            queue.append(child)

        while len(queue) > 0:
            item: Command = queue.popleft()
            blist.append(item)
            for child in self.tree.children(item.identifier):
                queue.append(child)

        return blist

    def __len__(self):
        # subtract root node
        return self.tree.__len__() - 1

    def get_item_for_uid(self, uid: UID) -> Command:
        return self.tree.get_node(uid)

    def get_total_completed(self) -> int:
        """Returns the number of commands which executed successfully"""
        total_succeeded: int = 0
        for command in self.get_breadth_first_list():
            if command.completed_without_error():
                total_succeeded += 1
        return total_succeeded

    def get_parent(self, uid: UID) -> Optional[Command]:
        parent = self.tree.parent(nid=uid)
        if parent and isinstance(parent, Command):
            return parent
        return None


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
    def __init__(self, uid, model_obj: DisplayNode, to_trash=True):
        super().__init__(uid, model_obj)
        self.to_trash = to_trash

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, context: CommandContext):
        try:
            logger.debug(f'RM: tgt={self._model.full_path}')
            file_util.delete_file(self._model.full_path, self.to_trash)

            context.cache_manager.remove_node(self._model, self.to_trash)

            self._status = CommandStatus.COMPLETED_OK
        except Exception as err:
            logger.exception(f'While deleting file: path={self._model.full_path}: {repr(err)}, to_trash={self.to_trash}')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, to_trash={self.to_trash}, ' \
               f'status={self._status}, model={self._model}'


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
            existing = context.gdrive_client.get_existing_file_with_parent_and_name(parent_goog_id=parent_goog_id, name=name)
            logger.debug(f'Found {len(existing.nodes)} existing files with parent={parent_goog_id} and name={name}')
            if self._overwrite:
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
                            # Update cache manager - it's likely out of date:
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
                    # Google will allow this, but it makes no sense when uploading a file from local disk
                    # (really, does it ever seem like a good idea?)
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
                        src_node_found = existing_data
                        break

            if src_node_found:
                context.gdrive_client.modify_meta(goog_id=self._model.src_node.goog_id, uid=self._model.uid,
                                                  remove_parents=[src_parent_goog_id],
                                                  add_parents=[dst_parent_goog_id], name=dst_name)

                goog_node = copy.copy(self._model.src_node)
                goog_node.name = self._model.name
                goog_node.parent_uids = self._model.parent_uids

                # Add node to disk & in-memory caches:
                context.cache_manager.add_or_update_node(goog_node)
                self._status = CommandStatus.COMPLETED_OK
                return
            else:
                # did not find the target file; see if our operation was already completed
                dest = context.gdrive_client.get_existing_file_with_parent_and_name(parent_goog_id=dst_parent_goog_id, name=dst_name)
                logger.debug(f'Found {len(dest.nodes)} matching dest files with parent={dst_parent_goog_id} and name={dst_name}')

                dst_node_found = None
                if len(existing.nodes) > 0:
                    for existing_data, existing_node in zip(existing.raw_items, existing.nodes):
                        assert isinstance(existing_node, GoogFile)
                        if existing_node.goog_id == self._model.src_node.goog_id:
                            dst_node_found = existing_data
                            break
                if dst_node_found:
                    logger.info(f'Identical already exists in Google Drive; will not update (goog_id={dst_node_found})')
                    # Update cache manager as it's likely out of date:
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
    def __init__(self, uid, model_obj: DisplayNode, to_trash=True):
        super().__init__(uid, model_obj)
        self.to_trash = to_trash

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, context: CommandContext):
        try:
            assert isinstance(self._model, GoogFile)
            existing_node, existing_parents = context.gdrive_client.get_meta_single_item_by_id(self._model.goog_id, self._model.uid)
            if not existing_node:
                raise RuntimeError('Cannot delete: not found in GDrive!')

            if self.to_trash and existing_node.trashed != NOT_TRASHED:
                logger.info(f'Item is already trashed: {existing_node}')
                self._status = CommandStatus.COMPLETED_NO_OP
                return

            if self.to_trash:
                context.gdrive_client.trash(self._model.goog_id)
                self._model.trashed = EXPLICITLY_TRASHED
            else:
                context.gdrive_client.hard_delete(self._model.goog_id)

            context.cache_manager.remove_node(self._model, self.to_trash)

        except Exception as err:
            logger.exception(f'While deleting from GDrive (to_trash={self.to_trash}, node={self._model}"')
            self._status = CommandStatus.STOPPED_ON_ERROR
            self._error = err

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, ' \
               f'status={self._status}, model={self._model}'


# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
# GOOGLE DRIVE COMMANDS end

