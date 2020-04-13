import logging
from stopwatch import Stopwatch
from fmeta.fmeta import FMetaTree, Category
from fmeta.fmeta_builder import FMetaDirScanner, FMetaDatabase
from ui.progress_meter import ProgressMeter

logger = logging.getLogger(__name__)


class FMetaTreeSource:
    def __init__(self, nickname, tree_root_path, enable_db_cache, db_cache_path):
        self.nickname = nickname
        self.tree_root_path = tree_root_path
        self.enable_db_cache = enable_db_cache
        self.db_cache_path = db_cache_path
        self.cache = FMetaDatabase(db_cache_path)

    def get_current_tree(self, status_receiver=None):
        tree: FMetaTree
        stopwatch = Stopwatch()
        if self.enable_db_cache:
            if self.cache.has_data():
                if status_receiver:
                    status_receiver.set_status(f'Loading {self.nickname} meta from cache: {self.db_cache_path}')
                tree = self.cache.load_fmeta_tree(self.tree_root_path)

            else:
                tree = self._scan_disk(status_receiver)
                self.cache.save_fmeta_tree(tree)
        else:
            tree = self._scan_disk(status_receiver)
        stopwatch.stop()
        logger.info(f'{self.nickname} loaded in: {stopwatch}')
        if status_receiver:
            status_receiver.set_status(tree.get_summary())
        return tree

    def _scan_disk(self, status_receiver=None):
        def on_progress_made(progress, total):
            if status_receiver:
                status_receiver.set_status(f'Scanning file {progress} of {total}')

        progress_meter = ProgressMeter(lambda p, t: on_progress_made(p, t))
        if status_receiver:
            status_msg = f'Scanning files in tree: {self.tree_root_path}'
            status_receiver.set_status(status_msg)
        dir_scanner = FMetaDirScanner(root_path=self.tree_root_path, progress_meter=progress_meter)
        return dir_scanner.scan_local_tree()
