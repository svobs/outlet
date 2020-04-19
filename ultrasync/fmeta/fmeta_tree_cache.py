import logging
from stopwatch import Stopwatch
from fmeta.fmeta import FMetaTree
from database import MetaDatabase
from ui import actions

logger = logging.getLogger(__name__)


def from_config(config, tree_id):
    enable_cache = config.get(f'transient.{tree_id}.cache.enable')
    if enable_cache:
        cache_file_path = config.get(f'transient.{tree_id}.cache.file_path')
        enable_load = config.get(f'transient.{tree_id}.cache.enable_load')
        enable_update = config.get(f'transient.{tree_id}.cache.enable_update')
        return SqliteCache(tree_id, cache_file_path, enable_load, enable_update)

    return NullCache()


class NullCache:
    """
    Acts as a cache for a single FMetaTree.
    """
    def __init__(self):
        pass

    def has_data(self):
        return False

    def load_fmeta_tree(self, root_path):
        return None

    def save_fmeta_tree(self, fmeta_tree):
        pass

    def overwrite_fmeta_tree(self, fmeta_tree):
        pass


class SqliteCache(NullCache):
    """
    Acts as a cache for a single FMetaTree.
    """
    def __init__(self, tree_id, db_file_path, enable_load, enable_update):
        super().__init__()
        self.tree_id = tree_id
        self.db_file_path = db_file_path
        self.enable_load = enable_load
        self.enable_update = enable_update

    def _open_db(self):
        return MetaDatabase(self.db_file_path)

    def load_fmeta_tree(self, root_path):
        fmeta_tree = FMetaTree(root_path)

        if not self.enable_load:
            return fmeta_tree

        db = self._open_db()
        try:
            if not db.has_file_changes():
                return fmeta_tree

            status = f'Loading {self.tree_id} meta from cache: {self.db_file_path}'
            logger.debug(status)
            if self.tree_id:
                actions.set_status(sender=self.tree_id, status_msg=status)

            db_file_changes = db.get_file_changes()
            if len(db_file_changes) == 0:
                raise RuntimeError('No data in database!')

            counter = 0
            for change in db_file_changes:
                meta = fmeta_tree.get_for_path(change.file_path)
                # Overwrite older changes for the same path:
                if meta is None or meta.sync_ts < change.sync_ts:
                    fmeta_tree.add(change)
                    counter += 1

            logger.debug(f'Reduced {str(len(db_file_changes))} DB entries into {str(counter)} entries')
            logger.info(fmeta_tree.get_stats_string())
            return fmeta_tree
        finally:
            db.close()

    def overwrite_fmeta_tree(self, fmeta_tree):
        if not self.enable_update:
            logger.debug(f'Skipping cache update for {self.tree_id} because it is disabled')
            return

        stopwatch_write_cache = Stopwatch()
        # Just overwrite all data - it's pretty fast, and less error prone:

        db = self._open_db()
        try:
            # Remove all rows
            db.truncate_file_changes()

            # Save all
            if db.has_file_changes():
                raise RuntimeError('Will not insert FMeta into DB! It is not empty')

            to_insert = fmeta_tree.get_all()
            db.insert_file_changes(to_insert)
            logger.info(f'Inserted {str(len(to_insert))} FMetas into previously empty DB table.')

            logger.info(f'{self.tree_id} updated cache in: {stopwatch_write_cache}')
        finally:
            db.close()
