from typing import Iterable, List, Optional
import logging

from backend.sqlite.op_db import OpDatabase
from constants import SUPER_DEBUG
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
        if self._db:
            self._db.close()
            self._db = None

    def cancel_pending_ops_from_disk(self):
        if SUPER_DEBUG:
            logger.debug('Entered cancel_pending_ops_from_disk()')

        op_list: List[UserOp] = self._db.get_all_pending_ops()
        if op_list:
            self._db.archive_failed_ops(op_list, 'Cancelled on startup per app_config')
            logger.info(f'Cancelled {len(op_list)} pending ops found in cache')
        else:
            logger.debug(f'Found no pending ops to cancel')

    def get_pending_ops_from_disk(self) -> List[UserOp]:
        if SUPER_DEBUG:
            logger.debug('Entered get_pending_ops_from_disk()')

        return self._db.get_all_pending_ops()

    def remove_pending_ops(self, op_list: Iterable[UserOp]):
        self._db.delete_pending_ops(op_list)

    def save_pending_ops_to_disk(self, op_list: Iterable[UserOp]):
        # This will save each of the planning nodes, if any:
        self._db.upsert_pending_ops(op_list, overwrite=False)

    def archive_pending_ops_to_disk(self, op_list: Iterable[UserOp]):
        self._db.archive_completed_ops(op_list)
