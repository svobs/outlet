import logging
import mimetypes
import os
from typing import List, Optional

from backend.executor.command.cmd_interface import Command, CommandContext, CopyNodeCommand, DeleteNodeCommand, UserOpResult, UserOpStatus
from constants import FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, GDRIVE_ME_USER_UID, TrashStatus
from error import GDriveItemNotFoundError, InvalidOperationError
from model.gdrive_meta import MimeType
from model.node.gdrive_node import GDriveFile, GDriveNode
from model.node.local_disk_node import LocalDirNode, LocalFileNode, LocalNode
from model.uid import UID
from model.user_op import UserOp, UserOpType
from util import file_util
from util.local_file_util import LocalFileUtil

logger = logging.getLogger(__name__)

# If true, raise exception if we see something unexpected, even if we could otherwise work around it:
USE_STRICT_STATE_ENFORCEMENT = True


# TODO: GDrive 'overwrite' logic. Include switch for choosing whether to delete an item only if you are unlinking it from its last parent,
#  or to delete it from all locations.
# TODO: also include both options in context menu

# FIXME: Handle GDrive shortcuts & Google Docs nodes differently - will these commands even work for them?

# TODO: resumable GDrive upload: https://developers.google.com/drive/api/v3/manage-uploads#python

# TODO: what if staging dir is not on same file system?


# LOCAL COMMANDS begin
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼


class CopyFileLocalToLocalCommand(CopyNodeCommand):
    """Local-to-local add or update"""

    def __init__(self, uid: UID, op: UserOp, overwrite: bool = False):
        super().__init__(uid, op, overwrite)
        assert op.op_type == UserOpType.CP

    def get_total_work(self) -> int:
        return self.op.src_node.get_size_bytes()

    def execute(self, cxt: CommandContext) -> UserOpResult:
        assert cxt
        assert isinstance(self.op.src_node, LocalFileNode), f'Got {self.op.src_node}'
        assert isinstance(self.op.dst_node, LocalFileNode), f'Got {self.op.dst_node}'

        local_file_util = LocalFileUtil(cxt.cacheman)
        src_path = self.op.src_node.get_single_path()
        dst_path = self.op.dst_node.get_single_path()
        src_node = local_file_util.ensure_up_to_date(self.op.src_node)
        to_upsert = [src_node]

        staging_path = os.path.join(cxt.staging_dir, src_node.md5)
        logger.debug(f'CP: src="{src_path}" stg="{staging_path}" dst="{dst_path}"')

        try:
            if self.overwrite:
                node_dst_old = cxt.cacheman.get_node_for_uid(self.op.dst_node.uid, self.op.dst_node.device_uid)
                node_dst_old = local_file_util.ensure_up_to_date(node_dst_old)

                local_file_util.copy_file_update(src_node=src_node, dst_node=node_dst_old, staging_path=staging_path,
                                                 verify=True, update_meta_also=cxt.update_meta_also)
            else:
                local_file_util.copy_file_new(src_node=src_node, dst_node=self.op.dst_node, staging_path=staging_path,
                                              verify=True, copy_meta_also=cxt.update_meta_also)

            result = UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=to_upsert)
        except file_util.IdenticalFileExistsError:
            # This is thrown if the file to be copied is already at the dst. Nothing to do.
            result = UserOpResult(UserOpStatus.COMPLETED_NO_OP, to_upsert=to_upsert)
            # However make sure we still keep the cache manager in the loop - it's likely out of date. Calculate fresh stats (below)

        # update cache:
        dst_node = cxt.cacheman.build_local_file_node(full_path=dst_path, is_live=True)
        assert dst_node.uid == self.op.dst_node.uid, f'LocalNode={dst_node}, DstNode={self.op.dst_node}'
        to_upsert.append(dst_node)

        return result


class DeleteLocalNodeCommand(DeleteNodeCommand):
    """
    Delete Local. This supports deleting either a single file or an empty dir.
    """

    def __init__(self, uid: UID, op: UserOp, to_trash=True):
        super().__init__(uid, op, to_trash)

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, LocalNode), f'Got {self.op.src_node}'
        local_file_util = LocalFileUtil(cxt.cacheman)

        if self.to_trash:
            # TODO: add support for local trash
            raise InvalidOperationError(f'to_trash==True not supported!')

        if self.op.src_node.is_file():
            assert isinstance(self.op.src_node, LocalFileNode), f'Got {self.op.src_node}'
            # make sure we are deleting the expected file:
            src_node = local_file_util.ensure_up_to_date(self.op.src_node)
            file_util.delete_file(src_node.get_single_path(), self.to_trash)
        elif self.op.src_node.is_dir():
            file_util.delete_empty_dir(self.op.src_node.get_single_path(), self.to_trash)
        else:
            raise RuntimeError(f'Not a file or dir: {self.op.src_node}')

        return UserOpResult(UserOpStatus.COMPLETED_OK, to_remove=[self.op.src_node])


class MoveFileLocalToLocalCommand(CopyNodeCommand):
    """
    Move/Rename Local -> Local
    """

    def __init__(self, uid: UID, op: UserOp, overwrite: bool):
        super().__init__(uid, op, overwrite)

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, LocalFileNode), f'Not a file: {self.op.src_node}'
        assert isinstance(self.op.dst_node, LocalFileNode), f'Not a file: {self.op.dst_node}'

        local_file_util = LocalFileUtil(cxt.cacheman)
        src_node = local_file_util.ensure_up_to_date(self.op.src_node)

        # Do the move:
        if self.overwrite:
            node_dst_old = cxt.cacheman.get_node_for_uid(self.op.dst_node.uid, self.op.dst_node.device_uid)

            if not node_dst_old:
                if USE_STRICT_STATE_ENFORCEMENT:
                    raise RuntimeError(f'Node no longer found in cache (using strict=true): {self.op.dst_node.node_identifier}')
                else:
                    file_util.move_file(self.op.src_node.get_single_path(), self.op.dst_node.get_single_path())
            else:
                local_file_util.ensure_up_to_date(node_dst_old)

                file_util.replace_file(self.op.src_node.get_single_path(), self.op.dst_node.get_single_path())
        else:
            file_util.move_file(self.op.src_node.get_single_path(), self.op.dst_node.get_single_path())

        if cxt.update_meta_also:
            # Moving a file shouldn't change its meta (except possibly on MacOS), but let's update it to be sure:
            local_file_util.copy_meta(src_node, self.op.dst_node.get_single_path())

        # Verify dst was created:
        new_dst_node: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=self.op.dst_node.get_single_path(), must_scan_signature=True,
                                                                         is_live=True)
        if not new_dst_node:
            raise RuntimeError(f'Dst node not found after move: {self.op.dst_node.get_single_path()}')
        assert new_dst_node.uid == self.op.dst_node.uid
        if not new_dst_node.is_signature_match(src_node):
            raise RuntimeError(f'Signature incorrect after move to "{self.op.dst_node.get_single_path()}" '
                               f'(expected: {src_node.md5}; found: {new_dst_node.md5})')

        # Verify src was deleted:
        if os.path.exists(src_node.get_single_path()):
            self._cleanup_after_error()
            raise RuntimeError(f'Src node still exists after move: {src_node.get_single_path()}')

        to_remove = [self.op.src_node]
        to_upsert = [new_dst_node]

        return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=to_upsert, to_remove=to_remove)

    def _cleanup_after_error(self):
        if os.path.exists(self.op.dst_node.get_single_path()):
            file_util.delete_file(self.op.dst_node.get_single_path(), to_trash=False)


class CreatLocalDirCommand(Command):
    """
    Create Local dir
    """

    def __init__(self, uid: UID, op: UserOp):
        super().__init__(uid, op)
        assert op.op_type == UserOpType.MKDIR

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def execute(self, cxt: CommandContext):
        logger.debug(f'MKDIR: dst={self.op.src_node.get_single_path()}')
        os.makedirs(name=self.op.src_node.get_single_path(), exist_ok=True)

        # Add to cache:
        assert isinstance(self.op.src_node, LocalDirNode)

        # Verify dst was created:
        new_dir_node: LocalDirNode = cxt.cacheman.build_local_dir_node(full_path=self.op.src_node.get_single_path(),
                                                                       is_live=True, all_children_fetched=True)

        # TODO: induce a failure here to test to make sure that a CP of dir properly cascades a failure

        if not new_dir_node:
            raise RuntimeError(f'Dir not found after MKDIR: {self.op.src_node.get_single_path()}')
        assert new_dir_node.uid == self.op.src_node.uid
        return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=[new_dir_node])


# ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
# LOCAL COMMANDS end


# LOCAL-GDRIVE COMMANDS begin
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

class CopyFileLocalToGDriveCommand(CopyNodeCommand):
    """
    Copy Local -> GDrive (AKA upload)
    """

    def __init__(self, uid: UID, op: UserOp, overwrite: bool, delete_src_node_after: bool):
        super().__init__(uid, op, overwrite)
        self.delete_src_node_after: bool = delete_src_node_after
        assert isinstance(self.op.dst_node, GDriveNode)

    def get_total_work(self) -> int:
        return self.op.src_node.get_size_bytes()

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, LocalFileNode), f'Expected LocalFileNode but got: {type(self.op.src_node)} for {self.op.src_node}'
        assert isinstance(self.op.dst_node, GDriveNode), f'Expected GDriveNode but got: {type(self.op.dst_node)} for {self.op.dst_node}'

        local_file_util = LocalFileUtil(cxt.cacheman)

        # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
        src_file_path: str = self.op.src_node.get_single_path()
        src_node = local_file_util.ensure_up_to_date(self.op.src_node)

        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.dst_node.device_uid)

        if self.overwrite:
            assert self.op.dst_node.goog_id, f'Expected dst node to have non-null goog_id because overwrite=true: {self.op.dst_node}'

            # look up by goog_id, see if node has already been updated
            existing_dst_node = gdrive_client.get_existing_node_by_id(self.op.dst_node.goog_id)

            if not existing_dst_node:
                # Possibility #1: no node at dst
                msg = f'Could not find target node to overwrite in Google Drive (using strict={USE_STRICT_STATE_ENFORCEMENT}): ' \
                      f'{self.op.dst_node.node_identifier}'
                if USE_STRICT_STATE_ENFORCEMENT:
                    raise RuntimeError(msg)
                else:
                    logger.debug(msg)
                    return self._upload_new_file(cxt, gdrive_client, src_node)
            elif self._relevant_fields_match(existing_dst_node, src_node):
                # Possibility #2: same exact node at dst -> no op
                logger.info(f'Identical node (uid={existing_dst_node.uid}) already exists in Google Drive with same name, '
                            f'md5={existing_dst_node.md5}, size={existing_dst_node.get_size_bytes()} as src node ({src_node})')
                return self._maybe_delete_src_node_and_finish(src_node, existing_dst_node, UserOpStatus.COMPLETED_NO_OP)

            if not self._relevant_fields_match(existing_dst_node, self.op.dst_node):
                # Possibility #3: unexpected content at dst
                msg = f'Dst node to overwrite has different meta/content than expected (using strict={USE_STRICT_STATE_ENFORCEMENT}): ' \
                      f'{self.op.dst_node.node_identifier}'
                if USE_STRICT_STATE_ENFORCEMENT:
                    raise RuntimeError(msg)
                else:
                    logger.debug(msg)

            # Possibility #4: found exactly what expected -> do the overwrite
            mime_type: MimeType = self._resolve_mime_type(gdrive_client, self.op.dst_node.mime_type_uid, src_node.get_single_path())

            existing_dst_node = gdrive_client.upload_update_to_existing_file(name=src_node.name, mime_type=mime_type.type_string,
                                                                             goog_id=self.op.dst_node.goog_id,
                                                                             local_file_full_path=src_file_path,
                                                                             create_ts=src_node.create_ts,
                                                                             modify_ts=src_node.modify_ts)
            if not self._relevant_fields_match(existing_dst_node, src_node):
                raise RuntimeError(f'Result of upload does not match expected: upload={existing_dst_node}, expected={src_node}')

            return self._maybe_delete_src_node_and_finish(src_node, existing_dst_node, UserOpStatus.COMPLETED_OK)
        else:
            assert not self.op.dst_node.goog_id, f'Expected dst node to have null goog_id because overwrite=false: {self.op.dst_node}'

            # Try to see if a node already exists at the given dst matching the properties of the src node
            existing_dst_node = gdrive_client.get_single_file_with_parent_and_name_and_criteria(self.op.dst_node)

            if existing_dst_node:
                if self._relevant_fields_match(existing_dst_node, src_node):
                    # Possibility #1: same exact node at dst -> no op
                    logger.info(f'Identical node (uid={existing_dst_node.uid}) already exists in Google Drive with same name and parent, '
                                f'md5={src_node.md5}, size={src_node.get_size_bytes()} (orig dst node uid={self.op.dst_node.uid})')
                    result: UserOpResult = self._maybe_delete_src_node_and_finish(src_node, existing_dst_node, UserOpStatus.COMPLETED_NO_OP)
                    result.nodes_to_remove.append(self.op.dst_node)
                    return result
                else:
                    logger.info(f'Found existing node in Google Drive with same parent and name, but different content (overwrite={self.overwrite})')
                    # Possibility #3: different node at dst AND overwrite=False -> fail
                    return self.set_error_result(f'While trying to add: found unexpected node(s) with the same name and parent: {existing_dst_node}')
            else:
                # Possibility #5: no node at dst -> OK to upload
                return self._upload_new_file(cxt, gdrive_client, src_node)

    @staticmethod
    def _relevant_fields_match(actual_node, expected_node) -> bool:
        return actual_node.name == expected_node.name and actual_node.is_signature_match(expected_node) and actual_node.meta_matches(expected_node)

    def _upload_new_file(self, cxt, gdrive_client, src_node) -> UserOpResult:
        parent_goog_id_list: List[str] = cxt.cacheman.get_parent_goog_id_list(self.op.dst_node)
        # Let Google figure out the mime_type (might want to change this later...)
        new_dst_node: GDriveFile = gdrive_client.upload_new_file(src_node.get_single_path(), parent_goog_ids=parent_goog_id_list,
                                                                 uid=self.op.dst_node.uid, create_ts=src_node.create_ts, modify_ts=src_node.modify_ts)
        assert new_dst_node.uid == self.op.dst_node.uid
        if not self._relevant_fields_match(new_dst_node, src_node):
            raise RuntimeError(f'Result of new file upload does not match expected: upload={new_dst_node}, expected={src_node}')
        return self._maybe_delete_src_node_and_finish(src_node, new_dst_node, UserOpStatus.COMPLETED_OK)

    def _maybe_delete_src_node_and_finish(self, node_src: LocalFileNode, node_dst: GDriveFile, status_up_to_now: UserOpStatus) -> UserOpResult:
        if not self.delete_src_node_after:
            return UserOpResult(status_up_to_now, to_upsert=[node_src, node_dst])

        assert isinstance(node_src, LocalFileNode)
        file_util.delete_file(node_src.get_single_path(), to_trash=False)
        return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=[node_dst], to_remove=[node_src])

    @staticmethod
    def _resolve_mime_type(gdrive_client, mime_type_uid: Optional[UID], file_path: str) -> MimeType:
        if mime_type_uid:
            mime_type: MimeType = gdrive_client.gdrive_store.get_mime_type_for_uid(mime_type_uid)
            if not mime_type:
                raise RuntimeError(f'Failed to resolve mime type for UID: {mime_type_uid}')
        else:
            mimetype_guess = mimetypes.guess_type(file_path)
            if not mimetype_guess:
                raise RuntimeError(f'Failed to guess MIMEType for path: "{file_path}"')
            mime_type_string = mimetype_guess[0]
            mime_type: MimeType = gdrive_client.gdrive_store.get_or_create_mime_type(mime_type_string)[0]
        return mime_type


class CopyFileGDriveToLocalCommand(CopyNodeCommand):
    """
    Copy GDrive -> Local (AKA download)
    """

    def __init__(self, uid: UID, op: UserOp, overwrite: bool, delete_src_node_after: bool):
        super().__init__(uid, op, overwrite)
        self.delete_src_node_after: bool = delete_src_node_after
        assert isinstance(self.op.src_node, GDriveNode), f'For {self.op.src_node}'
        assert isinstance(self.op.dst_node, LocalFileNode), f'For {self.op.dst_node}'

    def get_total_work(self) -> int:
        return self.op.src_node.get_size_bytes()

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveFile) and self.op.src_node.md5, f'Bad src node: {self.op.src_node}'
        src_goog_id = self.op.src_node.goog_id
        dst_path: str = self.op.dst_node.get_single_path()

        if os.path.exists(dst_path):
            node_dst: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=dst_path, must_scan_signature=True, is_live=True)
            if node_dst and node_dst.md5 == self.op.src_node.md5:
                logger.debug(f'Item already exists at path "{dst_path}" and appears valid: skipping download')
                return self._maybe_delete_src_node_and_finish(cxt, node_dst, UserOpStatus.COMPLETED_NO_OP)
            elif not self.overwrite:
                raise RuntimeError(f'A different node already exists at the destination path: {dst_path} '
                                   f'(found size={node_dst.get_size_bytes()}, MD5={node_dst.md5}, '
                                   f'expected size={self.op.src_node.get_size_bytes()}, MD5={self.op.src_node.md5})')
        elif self.overwrite:
            if USE_STRICT_STATE_ENFORCEMENT:
                raise RuntimeError(f'Cannot overwrite a file which does not exist (using strict=true): {dst_path}')
            else:
                logger.warning(f'Cmd has "overwrite" specified for a local file which does not exist: {dst_path}')

        # Set up staging vars:
        try:
            os.makedirs(name=cxt.staging_dir, exist_ok=True)
        except Exception:
            logger.error(f'Exception while making staging dir: {cxt.staging_dir}')
            raise
        staging_path = os.path.join(cxt.staging_dir, self.op.src_node.md5)

        # File already exists in staging with the given content?
        if os.path.exists(staging_path):
            node_dst: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=dst_path, staging_path=staging_path,
                                                                         must_scan_signature=True, is_live=True)
            if node_dst and node_dst.md5 == self.op.src_node.md5:
                logger.debug(f'Found target node in staging dir; will move: ({staging_path} -> {dst_path})')
                file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path, replace=self.overwrite)
                return self._maybe_delete_src_node_and_finish(cxt, node_dst, UserOpStatus.COMPLETED_OK)
            else:
                # probably a half-completed download
                logger.debug(f'Found unexpected file in the staging dir; removing: {staging_path}')
                os.remove(staging_path)

        # download into staging
        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.src_node.device_uid)
        gdrive_client.download_file(file_id=src_goog_id, dest_path=staging_path)

        # verify contents:
        node_dst: LocalFileNode = cxt.cacheman.build_local_file_node(full_path=dst_path, staging_path=staging_path,
                                                                     must_scan_signature=True, is_live=True)
        if node_dst.md5 != self.op.src_node.md5:
            raise RuntimeError(f'Downloaded MD5 ({node_dst.md5}) does not matched expected ({self.op.src_node.md5})!')

        # This will overwrite if the file already exists:
        file_util.move_to_dst(staging_path=staging_path, dst_path=dst_path, replace=self.overwrite)

        if cxt.update_meta_also:
            local_file_util = LocalFileUtil(cxt.cacheman)
            local_file_util.copy_meta(self.op.src_node, self.op.dst_node.get_single_path())

        return self._maybe_delete_src_node_and_finish(cxt, node_dst, UserOpStatus.COMPLETED_OK)

    def _maybe_delete_src_node_and_finish(self, cxt: CommandContext, node_dst: LocalFileNode, status_up_to_now: UserOpStatus) -> UserOpResult:
        if not self.delete_src_node_after:
            return UserOpResult(status_up_to_now, to_upsert=[self.op.src_node, node_dst])

        assert isinstance(self.op.src_node, GDriveFile)
        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.src_node.device_uid)

        existing_src_node = gdrive_client.get_existing_node_by_id(self.op.src_node.goog_id)
        if not existing_src_node:
            if USE_STRICT_STATE_ENFORCEMENT:
                raise RuntimeError(f'Could not find expected src node in Google Drive (using strict=true) with goog_id={self.op.src_node.goog_id}')
            else:
                logger.info(f'Could not find expected src node in Google Drive (goog_id={self.op.src_node.goog_id}): will skip delete of it')
                return UserOpResult(status_up_to_now, to_upsert=[node_dst], to_remove=[self.op.src_node])
        else:
            gdrive_client.hard_delete(self.op.src_node.goog_id)
            return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=[node_dst], to_remove=[self.op.src_node])


# PURE GDRIVE COMMANDS begin
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

class CreateGDriveFolderCommand(Command):
    """
    Create GDrive FOLDER (sometimes a prerequisite to uploading a file)
    """

    def __init__(self, uid: UID, op: UserOp):
        super().__init__(uid, op)
        assert op.op_type == UserOpType.MKDIR

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveNode) and self.op.src_node.is_dir() and not self.op.src_node.goog_id, f'For {self.op.src_node}'

        parent_goog_id_list: List[str] = cxt.cacheman.get_parent_goog_id_list(self.op.src_node)
        if not parent_goog_id_list:
            raise RuntimeError(f'No parents found for: {self.op.src_node}')
        name = self.op.src_node.name
        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.src_node.device_uid)
        existing = gdrive_client.get_folders_with_parent_and_name(parent_goog_id=parent_goog_id_list[0], name=name)
        if len(existing.nodes) > 0:
            if len(existing.nodes) > 1:
                raise RuntimeError(f'Found {len(existing.nodes)} existing folders with parent={parent_goog_id_list[0]} and name="{name}"; '
                                   f'expected at most 1')

            existing_folder = existing.nodes[0]
            if existing_folder.uid != self.op.src_node.uid:
                raise RuntimeError(f'Found unexpected existing folder with parent={parent_goog_id_list[0]} and name="{name}": '
                                   f'uid={existing_folder.uid}, goog_id={existing_folder.goog_id}')

            logger.info(f'Looks like folder was already created: uid={existing_folder.uid}, goog_id={existing_folder.goog_id}')
            goog_node: GDriveNode = existing.nodes[0]
        else:
            goog_node = gdrive_client.create_folder(name=self.op.src_node.name, parent_goog_ids=parent_goog_id_list, uid=self.op.src_node.uid)
            logger.info(f'Created GDrive folder successfully: uid={goog_node.uid} name="{goog_node.name}", goog_id="{goog_node.goog_id}"')

        assert goog_node.is_dir()
        assert goog_node.get_parent_uids(), f'Expected some parent_uids for: {goog_node}'
        if goog_node.uid != self.op.src_node.uid:
            to_remove = [self.op.src_node]
        else:
            to_remove = []
        return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=[goog_node], to_remove=to_remove)


class MoveFileWithinGDriveCommand(CopyNodeCommand):
    """
    Move GDrive -> GDrive. Really, this command boils down to a change of parent.
    If overwrite=False, the dst node is expected to have the same UID & goog_id as the src node, but different parent node[s].
    If overwrite=True, the dst node should have a different UID & goog_id, and in which case we first delete the dst node, then we change the parent
    of the src node to the dst node's parent.
    """

    def __init__(self, uid: UID, op: UserOp, overwrite: bool):
        super().__init__(uid, op, overwrite)
        assert op.op_type == UserOpType.MV

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveFile) and self.op.src_node.uid and self.op.src_node.is_live(), f'Invalid: {self.op.src_node}'
        assert isinstance(self.op.dst_node, GDriveFile) and self.op.dst_node.uid, f'Invalid {self.op.dst_node}'
        assert self.op.src_node.device_uid == self.op.dst_node.device_uid, \
            f'Not the same device_uid: {self.op.src_node.node_identifier}, {self.op.dst_node.node_identifier}'
        assert not self.op.src_node.has_same_parents(self.op.dst_node), \
            f'Src ({self.op.src_node}) & dst ({self.op.dst_node}) should have different parents!'

        # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
        src_parent_goog_id_list: List[str] = cxt.cacheman.get_parent_goog_id_list(self.op.src_node)
        dst_parent_goog_id_list: List[str] = sorted(cxt.cacheman.get_parent_goog_id_list(self.op.dst_node))

        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.src_node.device_uid)
        existing_src_node = gdrive_client.get_existing_node_by_id(self.op.src_node.goog_id)
        if not existing_src_node:
            raise RuntimeError(f'Could not find expected src node in Google Drive (goog_id={self.op.src_node.goog_id})')

        if self.overwrite:
            assert self.op.src_node.uid != self.op.dst_node.uid, \
                f'Src ({self.op.src_node}) & dst ({self.op.dst_node}) should have different UID for overwrite={self.overwrite}!'
            assert self.op.src_node.goog_id != self.op.dst_node.goog_id, \
                f'Src ({self.op.src_node}) & dst ({self.op.dst_node}) should have different goog_id for overwrite={self.overwrite}!'

            existing_dst_node = gdrive_client.get_existing_node_by_id(self.op.dst_node.goog_id)
            if not existing_dst_node:
                if USE_STRICT_STATE_ENFORCEMENT:
                    raise RuntimeError(f'Could not find expected dst node in Google Drive (using strict=true)'
                                       f' with goog_id={self.op.dst_node.goog_id}')
                else:
                    logger.info(f'Could not find expected dst node in Google Drive (goog_id={self.op.dst_node.goog_id}): will skip delete of it')
            else:
                # TODO: in future, let's find a way to create a new version of the existing file, rather than doing an explicit delete
                gdrive_client.hard_delete(self.op.dst_node.goog_id)
        else:
            assert self.op.src_node.uid == self.op.dst_node.uid, \
                f'Src ({self.op.src_node}) & dst ({self.op.dst_node}) should have same UID for overwrite={self.overwrite}!'
            assert self.op.src_node.goog_id == self.op.dst_node.goog_id, \
                f'Src ({self.op.src_node}) & dst ({self.op.dst_node}) should have same goog_id for overwrite={self.overwrite}!'

        existing_parent_goog_id_list = sorted(cxt.cacheman.get_goog_id_list_for_uid_list(existing_src_node.get_parent_uids()))
        if existing_parent_goog_id_list == dst_parent_goog_id_list:
            # Update cache manager as it's likely out of date:
            logger.info(f'Node in Google Drive (goog_id={existing_src_node.goog_id}) already has the correct parents ({dst_parent_goog_id_list})')
            return UserOpResult(UserOpStatus.COMPLETED_NO_OP, to_upsert=[existing_src_node], to_remove=[])

        goog_node = gdrive_client.modify_meta(goog_id=self.op.src_node.goog_id, remove_parents=[src_parent_goog_id_list],
                                              add_parents=[dst_parent_goog_id_list], new_name=self.op.dst_node.name,
                                              create_ts=self.op.src_node.create_ts, modify_ts=self.op.src_node.modify_ts)

        assert goog_node.name == self.op.dst_node.name and goog_node.uid == self.op.src_node.uid and goog_node.goog_id == self.op.src_node.goog_id, \
            f'Bad result: {goog_node}'

        # TODO: verify meta

        # Update master cache. Treat as an upsert, and let the master cache figure out what nodes need to be removed
        return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=[goog_node])


class CopyFileWithinGDriveCommand(CopyNodeCommand):
    """
    Copy GDrive -> GDrive, same account
    """

    def __init__(self, uid: UID, op: UserOp, overwrite: bool = False):
        super().__init__(uid, op, overwrite)
        assert op.op_type == UserOpType.CP

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.dst_node, GDriveFile), f'For {self.op.dst_node}'
        assert isinstance(self.op.src_node, GDriveFile), f'For {self.op.src_node}'
        # this requires that any parents have been created and added to the in-memory cache (and will fail otherwise)
        dst_parent_goog_id_list: List[str] = cxt.cacheman.get_parent_goog_id_list(self.op.dst_node)
        dst_name = self.op.dst_node.name

        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.src_node.device_uid)
        existing_src = gdrive_client.get_existing_node_by_id(self.op.src_node.goog_id)
        if not existing_src:
            raise RuntimeError(f'Could not find src node for copy in Google Drive: "{self.op.src_node.name}" (goog_id={self.op.src_node.goog_id})')

        existing_dst = gdrive_client.get_single_file_with_parent_and_name_and_criteria(self.op.dst_node)
        if existing_dst:
            if existing_dst.md5 == self.op.src_node.md5 and existing_dst.mime_type_uid == self.op.src_node.mime_type_uid:
                logger.info(f'File with identical content and name already exists in Google Drive (goog_id={existing_dst.goog_id})')

                to_upsert = [self.op.src_node, existing_dst]
                to_remove = []
                if existing_dst.uid != self.op.dst_node.uid:
                    to_remove.append(self.op.dst_node)

                return UserOpResult(UserOpStatus.COMPLETED_NO_OP, to_upsert=to_upsert, to_remove=to_remove)

        # Check whether node already exists at dst, and that it is as expected. Delete if specified.
        if self.overwrite:
            # Overwrite existing: dst_node must have a different goog_id
            if not self.op.dst_node.goog_id:
                raise RuntimeError(f'Cannot overwrite file in GDrive: no goog_id provided in dst node: {self.op.dst_node}')

            node_dst_updated = gdrive_client.get_existing_node_by_id(self.op.dst_node.goog_id)
            if node_dst_updated:
                assert node_dst_updated.uid == self.op.dst_node.uid and node_dst_updated.goog_id == self.op.dst_node.goog_id, \
                    f'Expected uid and goog_id of node from server {node_dst_updated} to match param {self.op.dst_node}'
            else:
                if USE_STRICT_STATE_ENFORCEMENT:
                    raise RuntimeError(
                        f'Cannot overwrite file in GDrive: target not found in Google Drive (maybe already deleted?) (using strict=true):'
                        f' {self.op.dst_node}')
                else:
                    logger.info(f'Could not find expected dst node in Google Drive (goog_id={self.op.dst_node.goog_id}): will skip delete of it')

            # TODO: in future, let's find a way to create a new version of the existing file, rather than doing an explicit delete
            gdrive_client.hard_delete(self.op.dst_node.goog_id)
        else:
            # Do not overwrite: dst_node should not have goog_id:
            if self.op.dst_node.goog_id:
                raise RuntimeError(f'Internal error: trying to overwrite existing GDrive node when overwrite==false: {self.op.dst_node}')

        # Do the copy
        new_node = gdrive_client.copy_existing_file(src_goog_id=self.op.src_node.goog_id, new_name=dst_name,
                                                    new_parent_goog_ids=dst_parent_goog_id_list, uid=self.op.dst_node.uid)
        if not new_node:
            raise RuntimeError(f'Copy failed to return a new Google Drive node! (src_id={self.op.src_node.goog_id})')

        # TODO: verify meta

        if new_node.uid != self.op.dst_node.uid:
            assert self.overwrite, f'new_node={new_node}, orig_dst={self.op.dst_node}'
            # The dst_node must be removed (it has a different UID)
            to_remove = [self.op.dst_node]
        else:
            to_remove = []

        # Update master cache. The dst_node must be removed (it has a different UID). The src_node will be updated with new icon
        return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=[self.op.src_node, new_node], to_remove=to_remove)


class DeleteGDriveNodeCommand(DeleteNodeCommand):
    """
    Delete GDrive Node. This supports either deleting a file or an empty folder.
    """

    def __init__(self, uid: UID, op: UserOp, to_trash=True):
        super().__init__(uid, op, to_trash)

    def get_total_work(self) -> int:
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def needs_gdrive(self):
        return True

    def execute(self, cxt: CommandContext):
        assert isinstance(self.op.src_node, GDriveNode)

        gdrive_client = cxt.cacheman.get_gdrive_client(self.op.src_node.device_uid)

        existing = gdrive_client.get_existing_node_by_id(self.op.src_node.goog_id)
        if not existing:
            return UserOpResult(UserOpStatus.COMPLETED_NO_OP, to_remove=[self.op.src_node])

        if existing.owner_uid != GDRIVE_ME_USER_UID:
            logger.warning(f'It appears the user does not own this file! We will see if this works (owner_uid={existing.owner_uid})')

        if self.to_trash and existing.get_trashed_status() != TrashStatus.NOT_TRASHED:
            logger.info(f'Item is already trashed: {existing}')
            return UserOpResult(UserOpStatus.COMPLETED_NO_OP, to_upsert=[existing])

        if self.to_trash:
            node_updated = gdrive_client.trash(self.op.src_node.goog_id)
            to_upsert = [node_updated]

            # Verify that Google did what it said it did...
            # TODO: this is really time consuming and we probably don't need it - revisit with testing
            for descendant in gdrive_client.get_subtree_bfs_node_list(self.op.src_node.goog_id):
                if descendant.get_trashed_status() == TrashStatus.NOT_TRASHED:
                    raise RuntimeError(f'Found a descendant ("{descendant.name}", id={descendant.goog_id}) which was not already trashed')

                to_upsert.append(descendant)

            return UserOpResult(UserOpStatus.COMPLETED_OK, to_upsert=to_upsert)
        else:
            existing_child_list: List[GDriveNode] = gdrive_client.get_all_children_for_parent(self.op.src_node.goog_id)
            if len(existing_child_list) > 0:
                raise RuntimeError(f'Folder has {len(existing_child_list)} children; will not delete non-empty folder ({self.op.src_node})')

            try:
                gdrive_client.hard_delete(self.op.src_node.goog_id)
            except GDriveItemNotFoundError:
                logger.warning(f'GDrive item not found while deleting: {self.op.src_node.goog_id} - assuming it was deleted externally')
                return UserOpResult(UserOpStatus.COMPLETED_OK, to_remove=[self.op.src_node])

            return UserOpResult(UserOpStatus.COMPLETED_OK, to_remove=[self.op.src_node])

# ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
# PURE GDRIVE COMMANDS end
