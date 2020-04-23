import logging
import os
from fmeta.fmeta import FMeta, FMetaTree, Category
import file_util
from ui import actions

logger = logging.getLogger(__name__)

FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT = 100  # TODO: put in config


class FMetaNoOp:
    def __init__(self, fm):
        self.fm = fm


class FMetaError(FMetaNoOp):
    def __init__(self, fm, exception):
        super().__init__(fm)
        self.exception = exception


def apply_changes_atomically(tree_id, tree: FMetaTree, staging_dir, continue_on_error=False, error_collector=None):

    if tree_id:
        # Get total byte count, for progress meter:
        total_bytes = 0
        for fmeta in tree.get_for_cat(Category.Added):
            total_bytes += fmeta.size_bytes
        for fmeta in tree.get_for_cat(Category.Updated):
            total_bytes += fmeta.size_bytes
        total_bytes += FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT * len(tree.get_for_cat(Category.Deleted))
        total_bytes += FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT * len(tree.get_for_cat(Category.Moved))
        actions.get_dispatcher().send(actions.START_PROGRESS, sender=tree_id, total=total_bytes)
        logger.debug(f'Total progress to make: {total_bytes}')

    # TODO: deal with file-not-found errors in a more robust way
    for fmeta in tree.get_for_cat(Category.Added):
        try:
            src_path = os.path.join(tree.root_path, fmeta.prev_path)
            dst_path = os.path.join(tree.root_path, fmeta.file_path)
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(staging_dir, fmeta.signature)
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
            logger.error(f'Exception occurred while processing Added file: root="{tree.root_path}", file_path="{fmeta.file_path}", prev_path="{fmeta.prev_path}": {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, progress=fmeta.size_bytes)

    for fmeta in tree.get_for_cat(Category.Deleted):
        try:
            tgt_path = os.path.join(tree.root_path, fmeta.file_path)
            logger.debug(f'RM: tgt={tgt_path}')
            file_util.delete_file(tgt_path)
        except Exception as err:
            logger.error(f'Exception occurred while processing Deleted file: root="{tree.root_path}", file_path={fmeta.file_path}: {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, progress=FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT)

    for fmeta in tree.get_for_cat(Category.Moved):
        try:
            src_path = os.path.join(tree.root_path, fmeta.prev_path)
            dst_path = os.path.join(tree.root_path, fmeta.file_path)
            logger.debug(f'MV: src={src_path}')
            logger.debug(f'    dst={dst_path}')
            file_util.move_file(src_path, dst_path)
        except Exception as err:
            logger.error(f'Exception occurred while processing Moved file: root="{tree.root_path}", file_path="{fmeta.file_path}", prev_path="{fmeta.prev_path}": {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, progress=FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT)

    for fmeta in tree.get_for_cat(Category.Updated):
        try:
            src_path = os.path.join(tree.root_path, fmeta.prev_path)
            dst_path = os.path.join(tree.root_path, fmeta.file_path)
            # TODO: what if staging dir is not on same file system?
            staging_path = os.path.join(staging_dir, fmeta.signature)
            logger.debug(f'CP: src={src_path}')
            logger.debug(f'    stg={staging_path}')
            logger.debug(f'    dst={dst_path}')
            file_util.copy_file_linux_with_attrs(src_path, staging_path, dst_path, fmeta, True)
        except file_util.IdenticalFileExistsError:
            if error_collector is not None:
                error_collector.append(FMetaNoOp(fm=fmeta))
        except Exception as err:
            # Try to log helpful info
            logger.error(f'Exception occurred while processing Updated file: root="{tree.root_path}", file_path="{fmeta.file_path}", prev_path="{fmeta.prev_path}": {repr(err)}')
            if continue_on_error:
                if error_collector is not None:
                    error_collector.append(FMetaError(fm=fmeta, exception=err))
            else:
                raise
        if tree_id:
            actions.get_dispatcher().send(actions.PROGRESS_MADE, sender=tree_id, progress=fmeta.size_bytes)

