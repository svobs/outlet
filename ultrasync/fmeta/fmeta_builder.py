#!/usr/bin/python3

# exiftool metadata:
# exiftool -AllDates="2001:01:01 12:00:00" *
# exiftool -Comment="Hawaii" ./2001\ Hawaii\ *
# find . -name "*jpg_original" -exec rm -fv {} \;

# Compare the output of this script with `tree -a .`
import sys
import os
import errno
from datetime import datetime
import time
import logging
import file_util
from fmeta.fmeta import FMeta, FMetaTree, Category
from fmeta.tree_recurser import TreeRecurser
import fmeta.content_hasher
from database import MetaDatabase
from pathlib import Path

from ui.progress_meter import ProgressMeter

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef')

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

logger = logging.getLogger(__name__)


def build_sync_item(root_path, file_path, category=Category.NA):
    if category == Category.Ignored:
        # Do not scan ignored files for content (optimization)
        signature_str = None
    else:
        # Open,close, read file and calculate hash of its contents
        signature_str = fmeta.content_hasher.dropbox_hash(file_path)

    relative_path = file_util.strip_root(file_path, root_path)

    # Get "now" in UNIX time:
    date_time_now = datetime.now()
    sync_ts = int(time.mktime(date_time_now.timetuple()))
    size_bytes = os.stat(file_path).st_size

    stat = os.stat(file_path)
    modify_ts = int(stat.st_mtime)
    change_ts = int(stat.st_ctime)

    return FMeta(signature_str, size_bytes, sync_ts, modify_ts, change_ts, relative_path, category)


# SUPPORT CLASSES ####################


class FileCounter(TreeRecurser):
    def __init__(self, root_path):
        TreeRecurser.__init__(self, root_path, valid_suffixes=VALID_SUFFIXES)
        self.files_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1


########################################################
# FMetaDirScanner: build FMetaTree from local tree
class FMetaDirScanner(TreeRecurser):
    def __init__(self, root_path, progress_meter: ProgressMeter):
        TreeRecurser.__init__(self, Path(root_path), valid_suffixes=VALID_SUFFIXES)
        self.progress_meter = progress_meter
        self.fmeta_tree = FMetaTree(root_path)

    def handle_target_file_type(self, file_path):
        item = build_sync_item(root_path=str(self.root_path), file_path=file_path)
        self.fmeta_tree.add(item)
        self.progress_meter.add_progress(1)

    def handle_non_target_file(self, file_path):
        item = build_sync_item(root_path=str(self.root_path), file_path=file_path, category=Category.Ignored)
        self.fmeta_tree.add(item)

    def scan_local_tree(self):
        root_path = str(self.root_path)
        if not os.path.exists(root_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), root_path)
        local_path = Path(root_path)
        # First survey our local files:
        logger.info(f'Scanning path: {local_path}')
        file_counter = FileCounter(local_path)
        file_counter.recurse_through_dir_tree()
        logger.debug(f'Found {file_counter.files_to_scan} files to scan.')
        if self.progress_meter is not None:
            self.progress_meter.set_total(file_counter.files_to_scan)
        self.recurse_through_dir_tree()
        logger.info(self.fmeta_tree.get_stats_string())
        return self.fmeta_tree


#####################################################
# FMetaDirScanner: build FMetaTree from previously built set in database
class FMetaDatabase:
    def __init__(self, db_file_path):
        self.db = MetaDatabase(db_file_path)

    def has_data(self):
        return self.db.has_file_changes()

    def load_fmeta_tree(self, root_path):
        fmeta_tree = FMetaTree(root_path)

        db_file_changes = self.db.get_file_changes()
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

    def save_fmeta_tree(self, fmeta_tree):
        if self.has_data():
            raise RuntimeError('Will not insert FMeta into DB! It is not empty')

        to_insert = fmeta_tree.get_all()
        self.db.insert_file_changes(to_insert)
        logger.info(f'Inserted {str(len(to_insert))} FMetas into previously empty DB table.')
