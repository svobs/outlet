import os
from typing import Iterable, List, Optional
import logging

from constants import OPS_FILE_NAME, SUPER_DEBUG
from model.user_op import UserOp
from store.sqlite.op_db import OpDatabase
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS OpDiskStore
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OpDiskStore(HasLifecycle):
    """Wrapper for OpDatabase; adds lifecycle and possibly complex logic"""
    def __init__(self, app):
        HasLifecycle.__init__(self)
        self.app = app
        self.op_db_path = os.path.join(self.app.cacheman.cache_dir_path, OPS_FILE_NAME)
        self._db: Optional[OpDatabase] = None

    def start(self):
        HasLifecycle.start(self)
        self._db = OpDatabase(self.op_db_path, self.app)

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
            self._db.archive_failed_ops(op_list, 'Cancelled on startup per user config')
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