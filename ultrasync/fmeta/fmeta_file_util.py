import logging
import os
import uuid

from constants import FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT
from model.fmeta import Category
import file_util
from model.fmeta_tree import FMetaTree
from model.planning_node import FileToAdd, FileToMove
from ui import actions

logger = logging.getLogger(__name__)


class FMetaNoOp:
    def __init__(self, fm):
        self.fm = fm


class FMetaError(FMetaNoOp):
    def __init__(self, fm, exception):
        super().__init__(fm)
        self.exception = exception


def apply_changes_atomically(tree_id, tree: FMetaTree, staging_dir, continue_on_error=False, error_collector=None):
    tx_id = uuid.uuid1()

    if tree_id:
        # Get total byte count, for progress meter:
        total_bytes = 0
        for fmeta in tree.get_for_cat(Category.Added):
            total_bytes += fmeta.size_bytes
        for fmeta in tree.get_for_cat(Category.Updated):
            total_bytes += fmeta.size_bytes
        total_bytes += FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT * len(tree.get_for_cat(Category.Deleted))
        total_bytes += FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT * len(tree.get_for_cat(Category.Moved))
        actions.get_dispatcher().send(actions.START_PROGRESS, sender=tree_id, tx_id=tx_id, total=total_bytes)
        logger.debug(f'Total progress to make: {total_bytes}')

    # TODO: deal with file-not-found errors in a more robust way
    for fmeta in tree.get_for_cat(Category.Added):
        try:
            assert isinstance(fmeta, FileToAdd)
            src_path = fmeta.original_full_path
            dst_path = fmeta.dest_path
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(staging_dir, fmeta.md5)
            logger.debug(f'CP: src={src_path}')
            logger.debug(f'    stg={staging_path}')
            logger.debug(f'    dst={dst_path}')
            file_util.copy_file_linux_with_attrs(src_path, staging_path, dst_path, fmeta, True)
        except file_util.IdenticalFileExistsError:
            # Not a real error. Note and proceed
            if error_collector is not None:
                error_collector.append(FMetaNoOp(fm=fmeta))
        except Exception as err:
            # Try to log helpful info
            logger.error(f'Exception occurred while processing Added file: root="{tree.root_path}", file_path="{fmeta.dest_path}", orig_path="{fmeta.original_full_path}": {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, tx_id=tx_id, progress=fmeta.size_bytes)

    for fmeta in tree.get_for_cat(Category.Deleted):
        try:
            logger.debug(f'RM: tgt={fmeta.full_path}')
            file_util.delete_file(fmeta.full_path)
        except Exception as err:
            logger.error(f'Exception occurred while processing Deleted file: root="{tree.root_path}", file_path={fmeta.full_path}: {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, tx_id=tx_id, progress=FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT)

    for fmeta in tree.get_for_cat(Category.Moved):
        try:
            assert isinstance(fmeta, FileToMove)
            logger.debug(f'MV: src={fmeta.original_full_path}')
            logger.debug(f'    dst={fmeta.dest_path}')
            file_util.move_file(fmeta.original_full_path, fmeta.dest_path)
        except Exception as err:
            logger.error(f'Exception occurred while processing Moved file: root="{tree.root_path}",'
                         f' dest_path="{fmeta.dest_path}", orig_path="{fmeta.original_full_path}": {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, tx_id=tx_id, progress=FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT)

    for fmeta in tree.get_for_cat(Category.Updated):
        try:
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(staging_dir, fmeta.md5)
            logger.debug(f'CP: src={fmeta.prev_path}')
            logger.debug(f'    stg={staging_path}')
            logger.debug(f'    dst={fmeta.full_path}')
            file_util.copy_file_linux_with_attrs(fmeta.prev_path, staging_path, fmeta.full_path, fmeta, True)
        except file_util.IdenticalFileExistsError:
            if error_collector is not None:
                error_collector.append(FMetaNoOp(fm=fmeta))
        except Exception as err:
            # Try to log helpful info
            logger.error(f'Exception occurred while processing Updated file: root="{tree.root_path}", file_path="{fmeta.full_path}", prev_path="{fmeta.prev_path}": {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, tx_id=tx_id, progress=fmeta.size_bytes)

    if tree_id:
        actions.get_dispatcher().send(actions.STOP_PROGRESS, sender=tree_id, tx_id=tx_id)
        logger.debug(f'Sent signal {actions.STOP_PROGRESS} from {tree_id}')
