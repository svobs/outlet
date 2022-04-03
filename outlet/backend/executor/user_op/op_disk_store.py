from typing import Iterable, List, Optional
import logging

from backend.sqlite.op_db import OpDatabase
from logging_constants import SUPER_DEBUG_ENABLED
from model.user_op import UserOp
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


class OpDiskStore(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpDiskStore

    Wrapper for OpDatabase; adds lifecycle and possibly complex logic
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, op_db_path: str):
        HasLifecycle.__init__(self)
        self.backend = backend
        self.op_db_path: str = op_db_path
        self._db: Optional[OpDatabase] = None

    def start(self):
        HasLifecycle.start(self)
        self._db = OpDatabase(self.op_db_path, self.backend)

    def shutdown(self):
        HasLifecycle.shutdown(self)
        try:
            if self._db:
                self._db.close()
                self._db = None
        except (AttributeError, NameError):
            pass

    def cancel_all_pending_ops(self):
        if SUPER_DEBUG_ENABLED:
            logger.debug('Entered cancel_all_pending_ops()')

        op_list: List[UserOp] = self._db.get_all_pending_ops()
        if op_list:
            self._db.archive_failed_op_list(op_list, 'Cancelled on startup per app_config')
            logger.info(f'Cancelled {len(op_list)} pending ops found in cache')
        else:
            logger.debug(f'Found no pending ops to cancel')

    def load_all_pending_ops(self) -> List[UserOp]:
        """ Gets all pending ops from disk, filling in their src and dst nodes as well """
        if SUPER_DEBUG_ENABLED:
            logger.debug('Entered load_all_pending_ops()')

        return self._db.get_all_pending_ops()

    def delete_pending_op_list(self, op_list: Iterable[UserOp]):
        self._db.delete_pending_ops(op_list)

    def cancel_op_list(self, op_list: List[UserOp], reason_msg: str):
        if SUPER_DEBUG_ENABLED:
            logger.debug(f'Cancelling {len(op_list)} ops with reason="{reason_msg}"')

        self._db.archive_failed_op_list(op_list, f'Cancelled: {reason_msg}')
        logger.info(f'Cancelled and archived {len(op_list)} ops with reason={reason_msg}')

    def upsert_pending_op_list(self, op_list: Iterable[UserOp]):
        # This will save each of the planning nodes, if any:
        self._db.upsert_pending_op_list(op_list)

    def archive_completed_op_list(self, op_list: Iterable[UserOp]):
        self._db.archive_completed_op_list(op_list)

    def archive_completed_op_and_batch(self, op: UserOp):
        self._db.archive_completed_op_and_batch(op)
